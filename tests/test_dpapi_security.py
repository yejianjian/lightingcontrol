import sys
import os
import base64
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.security import encrypt_secret, decrypt_secret

class TestDpapiSecurity(unittest.TestCase):
    def test_dpapi_encrypt_decrypt(self):
        # 1. 正常字符串加密与解密
        passwords = [
            "Admin@123456",
            "NeuOPC_P@ssw0rd!#$%",
            "中文密码测试8888",
            "1",
            "a" * 128
        ]
        for pwd in passwords:
            enc = encrypt_secret(pwd)
            self.assertTrue(enc.startswith("dpapi:") or enc.startswith("b64:"))
            self.assertNotEqual(enc, pwd)
            dec = decrypt_secret(enc)
            self.assertEqual(dec, pwd, f"Failed for {pwd}: got {dec}")

    def test_empty_and_none(self):
        # 2. 空密码安全
        self.assertEqual(encrypt_secret(""), "")
        self.assertEqual(decrypt_secret(""), "")
        self.assertEqual(decrypt_secret(None), "")

    def test_legacy_format_backward_compatibility(self):
        # 3. 兼容旧版历史数据（旧版以未加前缀的 Base64 存储）
        legacy_pwd = "MyOldPassword999"
        legacy_b64 = base64.b64encode(legacy_pwd.encode('utf-8')).decode('ascii')
        self.assertEqual(decrypt_secret(legacy_b64), legacy_pwd)

        # 4. 兼容历史明文密码
        raw_pwd = "PurePlainTextPassword"
        self.assertEqual(decrypt_secret(raw_pwd), raw_pwd)

if __name__ == "__main__":
    unittest.main()
