using System;
using System.IO;
using System.Text;
using FastTests.Utils;
using FastTests.Voron;
using FastTests.Voron.FixedSize;
using Raven.Server.ServerWide;
using Sparrow;
using Sparrow.Server;
using Voron;
using Voron.Data;
using Voron.Impl.Paging;
using Xunit;
using Voron.Global;
using Xunit.Abstractions;

namespace FastTests.Sparrow
{
    public class EncryptionTests : StorageTest
    {
        public EncryptionTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public unsafe void WriteAndReadPageUsingCryptoPager()
        {
            using (var options = StorageEnvironmentOptions.ForPath(DataDir))
            {
                options.Encryption.MasterKey = Sodium.GenerateRandomBuffer((int)Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes());

                using (var innerPager = LinuxTestUtils.GetNewPager(options, DataDir, "Raven.Voron"))
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

        [Theory]
        [InlineDataWithRandomSeed]
        public unsafe void WriteSeekAndReadInTempCryptoStream(int seed)
        {
            using (var options = StorageEnvironmentOptions.ForPath(DataDir))
            using (var stream = new TempCryptoStream(Path.Combine(DataDir, "EncryptedTempFile")))
            {
                var r = new Random(seed);

                var bytes = new byte[r.Next(128, 1024 * 1024 * 4)];
                fixed (byte* b = bytes)
                {
                    Memory.Set(b, (byte)'I', bytes.Length);
                }
                var injectionBytes = Encoding.UTF8.GetBytes("XXXXXXX");

                stream.Write(bytes, 0, bytes.Length);

                var someRandomLocationInTheMiddle = r.Next(0, bytes.Length - 7);
                fixed (byte* b = bytes)
                {
                    // injecting 7 'X' characters
                    Memory.Set(b + someRandomLocationInTheMiddle, (byte)'X', 7);
                }

                // Writing the same 7 'x's to the stream
                stream.Seek(someRandomLocationInTheMiddle, SeekOrigin.Begin);
                stream.Write(injectionBytes, 0, injectionBytes.Length);

                // Reading the entire stream back.
                var readBytes = new byte[bytes.Length];
                stream.Seek(0, SeekOrigin.Begin);

                var count = readBytes.Length;
                var offset = 0;
                while (count > 0)
                {
                    var read = stream.Read(readBytes, offset, count);
                    count -= read;
                    offset += read;
                }
                Assert.Equal(bytes, readBytes);
            }
        }

        [Fact]
        public unsafe void RavenDB_15975()
        {
            using (var options = StorageEnvironmentOptions.ForPath(DataDir))
            {
                options.Encryption.MasterKey = Sodium.GenerateRandomBuffer((int)Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes());

                using (var innerPager = LinuxTestUtils.GetNewPager(options, DataDir, "Raven.Voron"))
                {
                    AbstractPager cryptoPager;
                    using (cryptoPager = new CryptoPager(innerPager))
                    {
                        using (var tx = new TempPagerTransaction(isWriteTransaction: true))
                        {
                            var overflowSize = 4 * Constants.Storage.PageSize + 100;

                            cryptoPager.EnsureContinuous(26, 5); 
                            var pagePointer = cryptoPager.AcquirePagePointerForNewPage(tx, 26, 5);

                            var header = (PageHeader*)pagePointer;
                            header->PageNumber = 26;
                            header->Flags = PageFlags.Overflow;
                            header->OverflowSize = overflowSize;

                            Memory.Set(pagePointer + PageHeader.SizeOf, (byte)'X', overflowSize);
                        }

                        using (var tx = new TempPagerTransaction())
                        {
                            var pagePointer = cryptoPager.AcquirePagePointer(tx, 26);

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
