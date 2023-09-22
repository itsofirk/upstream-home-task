class UpstreamError(Exception):
    pass


class DataLakeError(UpstreamError):
    pass


class ApiError(UpstreamError):
    pass
