class UpstreamError(Exception):
    pass


class FilesystemError(UpstreamError):
    pass


class ApiError(UpstreamError):
    pass
