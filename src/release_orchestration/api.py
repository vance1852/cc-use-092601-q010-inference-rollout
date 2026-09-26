"""无第三方依赖的容量预演与发布编排 HTTP JSON 接口。"""

from __future__ import annotations

import argparse
import json
import threading
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlparse

from .errors import ReleaseError, ValidationFailed
from .service import ReleaseService
from .storage import connect


@dataclass(frozen=True, slots=True)
class Response:
    status: int
    body: Mapping[str, Any]


class JsonApplication:
    def __init__(self, service: ReleaseService) -> None:
        self.service = service
        self._lock = threading.Lock()

    @staticmethod
    def _actor(headers: Mapping[str, str]) -> str:
        actor = headers.get("x-actor-id", "").strip()
        if not actor:
            raise ValidationFailed("缺少 X-Actor-Id")
        return actor

    @staticmethod
    def _json(body: bytes) -> dict[str, Any]:
        if not body:
            return {}
        try:
            value = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValidationFailed("请求体必须是 UTF-8 JSON 对象") from exc
        if not isinstance(value, dict):
            raise ValidationFailed("请求体必须是 JSON 对象")
        return value

    def handle(self, method: str, target: str, headers: Mapping[str, str] | None = None, body: bytes = b"") -> Response:
        # 单连接 SQLite：串行化请求，保证事务不被并发请求交叉。
        with self._lock:
            return self._route(method, target, headers, body)

    def _route(self, method: str, target: str, headers: Mapping[str, str] | None = None, body: bytes = b"") -> Response:
        normalized = {key.lower(): value for key, value in (headers or {}).items()}
        parsed = urlparse(target)
        path = parsed.path.rstrip("/") or "/"
        parts = [part for part in path.split("/") if part]
        try:
            if method == "GET" and path == "/health":
                return Response(200, {"status": "ok"})
            payload = self._json(body) if method in {"POST", "PUT", "PATCH"} else {}
            actor = self._actor(normalized)
            if method == "POST" and path == "/users":
                return Response(201, self.service.create_user(payload["user_id"], payload["display_name"], payload["role"]))
            if method == "POST" and path == "/model-profiles":
                return Response(201, self.service.create_profile(actor, payload))
            if method == "GET" and len(parts) == 2 and parts[0] == "model-profiles":
                return Response(200, self.service.get_profile(parts[1]))
            if method == "POST" and path == "/capacity-pools":
                return Response(201, self.service.create_pool(actor, payload))
            if method == "GET" and len(parts) == 2 and parts[0] == "capacity-pools":
                return Response(200, self.service.get_pool(parts[1]))
            if method == "POST" and len(parts) == 3 and parts[0] == "capacity-pools" and parts[2] == "maintenance":
                return Response(200, self.service.maintain_pool(actor, parts[1], payload))
            if method == "POST" and path == "/release-plans":
                return Response(201, self.service.create_plan(actor, payload))
            if method == "GET" and len(parts) == 2 and parts[0] == "release-plans":
                return Response(200, self.service.plan_detail(actor, parts[1]))
            if method == "POST" and len(parts) == 3 and parts[0] == "release-plans" and parts[2] == "confirm":
                return Response(200, self.service.confirm_plan(actor, parts[1], int(payload["expected_revision"])))
            if method == "POST" and len(parts) == 3 and parts[0] == "release-plans" and parts[2] == "start":
                return Response(200, self.service.start_plan(actor, parts[1], int(payload["expected_revision"])))
            if method == "POST" and len(parts) == 3 and parts[0] == "release-plans" and parts[2] == "advance":
                return Response(200, self.service.advance_plan(actor, parts[1], int(payload["expected_revision"]), payload["metric_summary"]))
            if method == "POST" and len(parts) == 3 and parts[0] == "release-plans" and parts[2] == "rollback":
                return Response(200, self.service.rollback_plan(actor, parts[1], payload))
            if method == "GET" and path == "/audit/chain":
                return Response(200, self.service.audit_chain(actor))
            return Response(404, {"error": {"code": "route_not_found", "message": "接口不存在"}})
        except ReleaseError as exc:
            return Response(exc.status, {"error": {"code": exc.code, "message": str(exc)}})
        except (KeyError, TypeError, ValueError) as exc:
            return Response(422, {"error": {"code": "invalid_request", "message": str(exc)}})


def make_handler(application: JsonApplication):
    class Handler(BaseHTTPRequestHandler):
        server_version = "ReleaseOrchestration/1"

        def do_GET(self) -> None:  # noqa: N802
            self._dispatch()

        def do_POST(self) -> None:  # noqa: N802
            self._dispatch()

        def _dispatch(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length) if length else b""
            response = application.handle(self.command, self.path, dict(self.headers.items()), body)
            encoded = json.dumps(response.body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            self.send_response(response.status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: object) -> None:
            return

    return Handler


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="启动容量预演与发布编排服务")
    parser.add_argument("--database", type=Path, default=Path("release_orchestration.sqlite3"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8083)
    args = parser.parse_args(argv)
    connection = connect(args.database)
    server = ThreadingHTTPServer((args.host, args.port), make_handler(JsonApplication(ReleaseService(connection))))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
