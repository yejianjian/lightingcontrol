import sys
import os
import base64
from typing import Optional
from utils.logger import global_logger

# 针对 Windows 平台使用原生 DPAPI (CryptProtectData / CryptUnprotectData)
# 零外部依赖，密码与本机用户账户安全绑定
_IS_WINDOWS = sys.platform == "win32"

if _IS_WINDOWS:
    import ctypes
    import ctypes.wintypes

    class _DATA_BLOB(ctypes.Structure):
        _fields_ = [
            ('cbData', ctypes.wintypes.DWORD),
            ('pbData', ctypes.POINTER(ctypes.c_char))
        ]

    _crypt32 = ctypes.windll.crypt32
    _kernel32 = ctypes.windll.kernel32

    def _dpapi_encrypt(data: bytes) -> bytes:
        blob_in = _DATA_BLOB(len(data), ctypes.cast(ctypes.create_string_buffer(data), ctypes.POINTER(ctypes.c_char)))
        blob_out = _DATA_BLOB()
        # CRYPTPROTECT_UI_FORBIDDEN = 0x1
        if not _crypt32.CryptProtectData(ctypes.byref(blob_in), "LightingControlSecret", None, None, None, 0x1, ctypes.byref(blob_out)):
            raise ctypes.WinError()
        buf = ctypes.string_at(blob_out.pbData, blob_out.cbData)
        _kernel32.LocalFree(blob_out.pbData)
        return buf

    def _dpapi_decrypt(data: bytes) -> bytes:
        blob_in = _DATA_BLOB(len(data), ctypes.cast(ctypes.create_string_buffer(data), ctypes.POINTER(ctypes.c_char)))
        blob_out = _DATA_BLOB()
        if not _crypt32.CryptUnprotectData(ctypes.byref(blob_in), None, None, None, None, 0x1, ctypes.byref(blob_out)):
            raise ctypes.WinError()
        buf = ctypes.string_at(blob_out.pbData, blob_out.cbData)
        _kernel32.LocalFree(blob_out.pbData)
        return buf


def encrypt_secret(plain: str) -> str:
    """
    加密敏感字符串（如 OPC 密码、退出密码）。
    在 Windows 上使用 DPAPI 加密并附加 'dpapi:' 前缀保存为 Base64；
    在非 Windows 平台降级为 Base64。
    """
    if not plain:
        return ""
    plain_bytes = plain.encode('utf-8')
    if _IS_WINDOWS:
        try:
            cipher_bytes = _dpapi_encrypt(plain_bytes)
            return "dpapi:" + base64.b64encode(cipher_bytes).decode('ascii')
        except Exception as e:
            global_logger.warning(f"DPAPI encryption failed, falling back to base64: {e}")
    # 降级处理
    return "b64:" + base64.b64encode(plain_bytes).decode('ascii')


def decrypt_secret(encoded: str) -> str:
    """
    解密敏感字符串。
    具备平滑的向后兼容性：
    1. 若为 'dpapi:' 开头：使用 DPAPI 解密；
    2. 若为 'b64:' 开头：使用 Base64 解码；
    3. 若为旧版无前缀字符串：尝试 Base64 解码；失败则作为明文兼容返回。
    """
    if not encoded:
        return ""

    # 1. DPAPI 加密数据
    if encoded.startswith("dpapi:"):
        raw_b64 = encoded[6:]
        try:
            cipher_bytes = base64.b64decode(raw_b64.encode('ascii'))
            plain_bytes = _dpapi_decrypt(cipher_bytes)
            return plain_bytes.decode('utf-8')
        except Exception as e:
            global_logger.error(f"DPAPI decryption failed: {e}")
            return ""

    # 2. b64 显式标记数据
    if encoded.startswith("b64:"):
        raw_b64 = encoded[4:]
        try:
            return base64.b64decode(raw_b64.encode('ascii')).decode('utf-8')
        except Exception:
            return ""

    # 3. 兼容旧版历史数据（旧版使用未加前缀的 base64 存储或直接明文）
    try:
        decoded_bytes = base64.b64decode(encoded.encode('ascii'))
        # 检验是否为合法 utf-8 字符串
        return decoded_bytes.decode('utf-8')
    except Exception:
        # 直接返回明文兼容
        return encoded
