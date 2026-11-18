import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from app.routers.clue_webhook import handle_webhook_exceptions
from app.utils.exceptions import StateTransitionError, LockTimeoutError

# 创建一个轻量级的测试专用 App
# 这里我们不引入真实的 app.main.app，是为了避免启动 Lifespan 连接真实数据库与 Redis
# 而是专门验证【拦截规则】的 HTTP 行为语义
mock_app = FastAPI()

@mock_app.get("/trigger-state-error")
@handle_webhook_exceptions
async def trigger_state_error():
    raise StateTransitionError("由于网络抖动，线索已经是 ACCEPTED，再次引发 CALLING 的非法流转", clue_id="mock_clue_123")

@mock_app.get("/trigger-timeout-error")
@handle_webhook_exceptions
async def trigger_timeout_error():
    raise LockTimeoutError("Redis获取分布式锁由于并发过高等待超出了5秒", clue_id="mock_clue_123")

client = TestClient(mock_app)

def test_state_transition_http_response():
    """
    测试目标1：防止上游系统的重试风暴（Retry Storm）
    当底层业务因为状态校验失败，抛出 StateTransitionError 时，
    FastAPI 必须伪装为 200 OK，并告知上游已经被忽略（拦截）。
    """
    response = client.get("/trigger-state-error")
    
    # HTTP 必须返回 200，否则外部外呼平台会无限轰炸我们
    assert response.status_code == 200
    
    data = response.json()
    assert data["code"] == 1
    assert "状态冲突/废弃" in data["msg"]
    assert "mock_clue_123" in data["msg"]


def test_lock_timeout_http_response():
    """
    测试目标2：符合 RESTful 标准的服务限流
    当并发挤压，由于获取不到系统排队锁抛出 LockTimeoutError 时，
    FastAPI 返回 200 OK 但内部携带错误码，触发上游回退队列退避稍后重发。
    """
    response = client.get("/trigger-timeout-error")
    
    # HTTP 必须返回 200
    assert response.status_code == 200
    
    data = response.json()
    assert data["code"] == 2
    assert "锁等待超时" in data["msg"]
    assert " mock_clue_123" in data["msg"]
