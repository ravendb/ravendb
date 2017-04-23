using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection.Metadata;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using FastTests.Voron;
using Sparrow;
using Sparrow.Platform;
using Voron;
using Voron.Data;
using Voron.Impl;
using Voron.Impl.Paging;
using Voron.Platform.Win32;
using Xunit;
using Voron.Global;

namespace FastTests.Sparrow
{
    public class EncryptionTests : StorageTest
    {
        [Fact]
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

        [Fact]
        public unsafe void WriteAndReadPageUsingCryptoPager()
        {
            using (var options = StorageEnvironmentOptions.ForPath(DataDir))
            {
                options.MasterKey = Sodium.GenerateMasterKey();

                using (var innerPager = new WindowsMemoryMapPager(options, Path.Combine(DataDir, "Raven.Voron")))
                {
                    AbstractPager cryptoPager;
                    using (cryptoPager = new CryptoPager(innerPager))
                    {
                        using (var tx = new TempPagerTransaction(isWriteTransaction: true))
                        {
                            cryptoPager.EnsureContinuous(17, 1); // We're gonna try to read and write to page 17
                            var pagePointer = cryptoPager.AcquirePagePointerForNewPage(tx, 17, 1);

                            var header = (PageHeader*)pagePointer;
                            header->PageNumber = 17;
                            header->Flags = PageFlags.Single | PageFlags.FixedSizeTreePage;

                            Memory.Set(pagePointer + PageHeader.SizeOf, (byte)'X', Constants.Storage.PageSize - PageHeader.SizeOf);
                        }

                        using (var tx = new TempPagerTransaction())
                        {
                            var pagePointer = cryptoPager.AcquirePagePointer(tx, 17);

                            // Making sure that the data was decrypted and still holds those 'X' chars
                            Assert.True(pagePointer[PageHeader.SizeOf] == 'X');
                            Assert.True(pagePointer[666] == 'X');
                            Assert.True(pagePointer[1039] == 'X');
                        }
                    }
                }
            }
        }
    }
}
