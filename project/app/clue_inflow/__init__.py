"""商业化线索受理、文件导入及入池恢复的公共业务异常。"""

class InflowError(Exception):
    """可公开的业务错误；不包含原始线索或底层异常内容。"""

    def __init__(self, code: int, message: str, status: int = 400, data=None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status
        self.data = data

    def response(self):
        return {"code": self.code, "msg": self.message, "data": self.data}
