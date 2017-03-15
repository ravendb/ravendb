using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using Sparrow;
using Xunit;

namespace FastTests.Sparrow
{
    public class EncryptionTests
    {
        [Fact(Skip = "Need to fix dll loading")]
        public void EncryptAndDecryptWithAdditionalData()
        {
            var nonce = Sodium.GenerateNonce();
            var key = Sodium.GenerateKey();
            var mac = new byte[16];
            var msg = "Hello my dear world";
            var message = Encoding.UTF8.GetBytes(msg);
            var now = DateTime.Today;
            var additionalData = BitConverter.GetBytes(now.Ticks);

            var crypt = Sodium.AeadChacha20Poly1305Encrypt(key, nonce, message, additionalData, mac);
            
            var plain = Sodium.AeadChacha20Poly1305Decrypt(key, nonce, crypt, additionalData, mac);

            var s = Encoding.UTF8.GetString(plain);
            Assert.Equal(msg, s);
        }

    }
}
