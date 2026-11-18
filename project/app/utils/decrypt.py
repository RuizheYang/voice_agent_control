import base64
import hashlib
import logging
import os
from typing import Optional

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from tortoise import fields

from app.config import config

logger = logging.getLogger(__name__)

KEY_VERSION_LABEL = config.security_config.version_label or "V1"
# 保持兼容导出名称
IT_VERSION_LABEL = KEY_VERSION_LABEL


def _resolve_aes_key(raw_key: Optional[str]) -> bytes:
    """
    将配置密钥解析为合法的 AES 密钥字节（16/24/32 字节）：
    - 未配置时使用开发测试默认密钥派生（非生产安全）；
    - 支持直接传入 16/24/32 字节字符串、Base64 字符串或任意长度口令（SHA-256 归一化为 16 字节）。
    """
    candidate = (raw_key or "").strip()
    if not candidate:
        logger.warning(
            "[Security] 未配置 APP_ENCRYPTION_KEY，当前使用开发默认密钥；生产部署请务必设置独立密钥！"
        )
        return hashlib.sha256(b"voice-agent-control-dev-only-key").digest()[:16]

    raw_bytes = candidate.encode("utf-8")
    if len(raw_bytes) in (16, 24, 32):
        return raw_bytes

    try:
        decoded = base64.b64decode(candidate, validate=True)
        if len(decoded) in (16, 24, 32):
            return decoded
    except Exception:
        pass

    return hashlib.sha256(raw_bytes).digest()[:16]


class DatabaseSecurityManager:
    """基于 AES-GCM 的字段级加密与解密管理器（格式：Version(2) + IV(12) + Ciphertext + Tag(16)）"""

    def __init__(self, key_map: dict[str, bytes], search_salt: str):
        self.key_map = key_map
        self.search_salt = search_salt

    def encrypt_field(self, plaintext: str, version_str: str = KEY_VERSION_LABEL) -> str:
        key = self.key_map.get(version_str)
        if not key:
            raise ValueError(f"未配置版本 {version_str} 的加密密钥")

        version_bytes = version_str.encode("ascii")
        if len(version_bytes) != 2:
            raise ValueError("密钥版本标识必须固定为 2 字节（例如 'V1'）")

        iv = os.urandom(12)
        cipher = Cipher(algorithms.AES(key), modes.GCM(iv), backend=default_backend())
        encryptor = cipher.encryptor()
        ciphertext = encryptor.update(plaintext.encode("utf-8")) + encryptor.finalize()
        combined = version_bytes + iv + ciphertext + encryptor.tag
        return base64.b64encode(combined).decode("utf-8")

    def decrypt_field(self, cipher_b64: str) -> str:
        cipher_b64 = cipher_b64.strip()
        raw = base64.b64decode(cipher_b64)
        version_str = raw[:2].decode("ascii")
        key = self.key_map.get(version_str)
        if not key:
            raise ValueError(f"密文版本标识 {version_str} 无对应密钥")

        iv = raw[2:14]
        tag = raw[-16:]
        ciphertext = raw[14:-16]
        decryptor = Cipher(algorithms.AES(key), modes.GCM(iv, tag), backend=default_backend()).decryptor()
        return (decryptor.update(ciphertext) + decryptor.finalize()).decode("utf-8")


_active_key = _resolve_aes_key(config.security_config.encryption_key)
crypted_manager = DatabaseSecurityManager(
    key_map={KEY_VERSION_LABEL: _active_key},
    search_salt=config.security_config.search_salt,
)


class EncryptedPhoneNumberField(fields.CharField):
    """自动加密/解密敏感字段（如手机号）的 Tortoise 字段"""

    def __init__(self, **kwargs):
        kwargs["max_length"] = 128
        super().__init__(**kwargs)
        self.security_manager = crypted_manager

    def to_db_value(self, value, instance):
        if value is None:
            return None
        if len(str(value)) > 20:
            return value
        return self.security_manager.encrypt_field(str(value), KEY_VERSION_LABEL)

    def to_python_value(self, value):
        if value is None or not value:
            return value
        try:
            return self.security_manager.decrypt_field(str(value))
        except Exception as exc:
            logger.warning("字段解密失败，回退返回原值: %r", exc)
            return value
