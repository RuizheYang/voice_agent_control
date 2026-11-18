"""总结前处理使用的话务码分类和非真人检测。"""
import re


PHONE_LIMIT_CODES = frozenset({"10006", "10008", "10009", "20008"})
NO_ANSWER_COMPAT_CODES = frozenset({"10005", "10012"})

# 仅检查被叫方发言；否认、提问和找人工的表达优先排除。
_WHITELIST = re.compile(r"你是.*?(?:AI|机器人|助理|智能)|我(?:不)?要找.*?客服|这是.*?语音吗|"
                        r"我不是.*?(?:AI|机器人|语音助手|智能助理)", re.IGNORECASE)
_VOICE_ASSISTANT = re.compile(
    r"^.{0,30}(?:我是.{0,12}(?:语音助手|智能助理|AI客服|AI助手|智能客服|机器人|通信管家|数字分身|小爱|小度)|"
    r"语音留言|滴声后留言|录制留言|"
    r"正在为您转接|联通助理|天逸通信助理|Thank you for calling|Please try again later)",
    re.IGNORECASE,
)


def normalize_code(value) -> str | None:
    """保留前导零；数值 0 不冒充平台的字符串 00000。"""
    if value is None:
        return None
    if not isinstance(value, str):
        return None
    return value.strip() or None


def classify_result_code(value) -> str:
    code = normalize_code(value)
    if code in PHONE_LIMIT_CODES:
        return "phone_limit"
    if code in NO_ANSWER_COMPAT_CODES:
        return "unanswered"
    if code == "00000":
        return "connected"
    if code == "20004":
        return "ring_rejected"
    if code and re.fullmatch(r"2000\d", code):
        return "unanswered"
    if code and re.fullmatch(r"1\d{4}", code):
        return "abnormal"
    return "unknown"


def is_voice_assistant(turns) -> bool:
    # 弱留言提示、秘书身份都可能来自真人；机器转接后也可能有真人接听。
    # 只有所有被叫发言均明确属于机器时才整通跳过，其余交给模型判断。
    user_turns = [turn for turn in turns if turn.role == "user" and turn.content]
    return bool(user_turns) and all(
        not _WHITELIST.search(turn.content) and _VOICE_ASSISTANT.search(turn.content)
        for turn in user_turns
    )
