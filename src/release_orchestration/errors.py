"""发布编排服务向 API 和 CLI 暴露的稳定错误。"""


class ReleaseError(RuntimeError):
    code = "release_error"
    status = 400


class NotFound(ReleaseError):
    code = "not_found"
    status = 404


class Conflict(ReleaseError):
    code = "conflict"
    status = 409


class Forbidden(ReleaseError):
    code = "forbidden"
    status = 403


class InvalidState(ReleaseError):
    code = "invalid_state"
    status = 409


class ValidationFailed(ReleaseError):
    code = "validation_failed"
    status = 422
