import base64
import logging
import os
import re
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from tortoise import fields
from app.config import config


class DatabaseSecurityManager:
    def __init__(self, key_map, search_salt):
        self.key_map = key_map  # 格式: {"V1": b'...', "V2": b'...'}
        self.search_salt = search_salt

    # ==========================================
    # 加密逻辑：严格遵循 Version(2) + IV(12) + Data + Tag(16)
    # ==========================================
    def encrypt_field(self, plaintext, version_str):
        """
        :param version_str: 传入 "V1" 或 "V2"
        """
        key = self.key_map.get(version_str)
        if not key: raise ValueError(f"未配置版本 {version_str} 的密钥")

        # 1. 准备 2 字节的版本标识 (严格按字符串转换)
        # 如果 version_str 是 "V1"，转化为 bytes 就是 b'V1'
        version_bytes = version_str.encode('ascii')
        if len(version_bytes) != 2:
            raise ValueError("版本标识必须固定为 2 字节 (例如 'V1')")

        # 2. 生成 12 字节 IV
        iv = os.urandom(12)

        # 3. AES-128-GCM 加密
        cipher = Cipher(algorithms.AES(key), modes.GCM(iv), backend=default_backend())
        encryptor = cipher.encryptor()
        ciphertext = encryptor.update(plaintext.encode('utf-8')) + encryptor.finalize()
        tag = encryptor.tag  # 16 字节 Tag

        # 4. 【核心步骤】按照文档规则拼接
        # 拼接顺序：Version(2) + IV(12) + Ciphertext + Tag(16)
        combined = version_bytes + iv + ciphertext + tag

        # 5. 转为 Base64 存储
        return base64.b64encode(combined).decode('utf-8')

    # ==========================================
    # 解密逻辑：严格拆分
    # ==========================================
    def decrypt_field(self, cipher_b64):
        cipher_b64 = cipher_b64.strip()
        raw = base64.b64decode(cipher_b64)

        # 1. 解析前 2 字节作为版本号
        version_str = raw[:2].decode('ascii')
        key = self.key_map.get(version_str)
        if not key: raise ValueError(f"数据库中的版本标识 {version_str} 无效")

        # 2. 严格按照字节长度拆分
        # IV 为 2 到 14 字节 (12位)
        # Tag 为最后 16 字节
        # 中间是密文
        iv = raw[2:14]
        tag = raw[-16:]
        ciphertext = raw[14:-16]

        # 3. 解密
        decryptor = Cipher(algorithms.AES(key), modes.GCM(iv, tag), backend=default_backend()).decryptor()
        return (decryptor.update(ciphertext) + decryptor.finalize()).decode('utf-8')


class EncryptedPhoneNumberField(fields.CharField):
    """
    自动加解密手机号字段
    存储格式: Version(2) + IV(12) + Ciphertext + Tag(16)
    """

    def __init__(self, **kwargs):
        # 考虑到加密后 Base64 的长度，将数据库实际长度设为 128
        kwargs['max_length'] = 128
        super().__init__(**kwargs)
        real_key = unwrap_it_key(config.security_config.encrypted_str, config.security_config.xor_key)
        self.security_manager = DatabaseSecurityManager(
            key_map={
                config.security_config.version_label: real_key
            },
            search_salt=config.security_config.search_salt
        )

    def to_db_value(self, value, instance):
        """存入数据库前：加密"""
        if value is None: return None
        # 如果已经是加密串（比如查询时传入的索引），则不重复处理
        if len(str(value)) > 20: return value

        return self.security_manager.encrypt_field(str(value), config.security_config.version_label)

    def to_python_value(self, value):
        """从数据库取出后：解密"""
        if value is None or not value: return value
        try:
            return self.security_manager.decrypt_field(str(value))
        except Exception as e:
            logging.warning(f"数据解密失败, 原值: {value}, 错误信息: {repr(e)}")
            return value

# ==========================================
# 密钥解密工具
# ==========================================
def _rotr(b, shift):
    """严格按照 8 位二进制循环右移"""
    return ((b >> shift) | (b << (8 - shift))) & 0xFF


def _xor_transform(data, key):
    """严格对齐文档阶段三：4步逆向变换"""
    k1 = ord(key[0]) & 0xFF
    k2 = ord(key[1]) & 0xFF
    k3 = ord(key[2]) & 0xFF

    result = bytearray()
    shift_val = (k2 % 7) + 1  # 文档定义的位移量

    for b in data:
        # 步骤 1: 抹除动态加法偏移
        b = (b - (k1 % 13)) & 0xFF
        # 步骤 2: 第一次异或
        b = b ^ k3
        # 步骤 3: 逆向循环移位
        b = _rotr(b, shift_val)
        # 步骤 4: 第二次异或
        b = b ^ k1
        result.append(b)
    return bytes(result)


def unwrap_it_key(encrypted_b64, xor_key):
    # 自动清洗：只保留 Base64 合法字符，防止空格/换行干扰
    clean_b64 = re.sub(r'[^A-Za-z0-9+/=]', '', encrypted_b64)

    data = base64.b64decode(clean_b64)

    # 严格拆分：12字节Nonce + 中间密文 + 16字节Tag
    nonce = data[:12]
    tag = data[-16:]
    ciphertext = data[12:-16]

    # AES-128-GCM 解密
    cipher = Cipher(
        algorithms.AES(xor_key.encode('utf-8')),
        modes.GCM(nonce, tag),
        backend=default_backend()
    )
    decryptor = cipher.decryptor()

    # 执行解密（InvalidTag 通常发生在这里）
    intermediate = decryptor.update(ciphertext) + decryptor.finalize()

    # 执行位运算变换
    return _xor_transform(intermediate, xor_key)


# IT 部门下发的信息
IT_VERSION_LABEL = "V1"  # 字符串格式
IT_XOR_KEY = "6b5eSBX6bIGjWRA0"
IT_ENCRYPTED_STR = "fwJ2m64g8nx4q3F3DwAqIKSKVuYc83QFfOuURzctiFyu7FtKDfORbi6/PdQ="

# 1. 解出真实密钥
real_key = unwrap_it_key(IT_ENCRYPTED_STR, IT_XOR_KEY)

# 2. 初始化管理器 (使用 "V1" 作为键)
crypted_manager = DatabaseSecurityManager(
    key_map={IT_VERSION_LABEL: real_key},
    search_salt="Global_Salt_2026"
)

# ==========================================
# 执行
# ==========================================
if __name__ == "__main__":
    # 3. 测试加密和拼接
    phone = "杨112"
    encrypted_data = crypted_manager.encrypt_field(phone, IT_VERSION_LABEL)

    print(f"[*] 密文 (Base64): {encrypted_data}")

    # 验证解密
    decrypted_data = crypted_manager.decrypt_field(encrypted_data)
    print(f"[*] 解密成功: {decrypted_data}")