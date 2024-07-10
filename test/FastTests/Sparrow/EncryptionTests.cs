using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Elastic.Clients.Elasticsearch;
using FastTests.Voron;
using FastTests.Voron.FixedSize;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Sparrow;
using Sparrow.Platform;
using Sparrow.Server.Platform;
using Sparrow.Utils;
using Tests.Infrastructure;
using Voron;
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

        [RavenMultiplatformFact(RavenTestCategory.Voron | RavenTestCategory.Encryption, RavenPlatform.Windows | RavenPlatform.Linux)]
        public unsafe void WriteAndReadPageUsingCryptoPager()
        {
            Options.Encryption.MasterKey = Sodium.GenerateRandomBuffer((int)Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes());

            var file = Env.ScratchBufferPool.GetScratchBufferFile(0);
            var (pager, state) = file.GetPagerAndState();

            Pager2.PagerTransactionState txState = new(){IsWriteTransaction = true};
            try
            {
                pager.EnsureContinuous(ref state, 17, 1); // We're gonna try to read and write to page 17
                var pagePointer = pager.AcquirePagePointerForNewPage(state, ref txState, 17, 1);

                var header = (PageHeader*)pagePointer;
                header->PageNumber = 17;
                header->Flags = PageFlags.Single | PageFlags.FixedSizeTreePage;

                Memory.Set(pagePointer + PageHeader.SizeOf, (byte)'X', Constants.Storage.PageSize - PageHeader.SizeOf);
                txState.InvokeBeforeCommitFinalization(Env, ref state, ref txState);
            }
            finally
            {
                txState.InvokeDispose(Env, ref state, ref txState);
            }

            txState = default;
            try
                {
                    var pagePointer = pager.AcquirePagePointerWithOverflowHandling(state, ref txState, 17);

                    // Making sure that the data was decrypted and still holds those 'X' chars
                    Assert.True(pagePointer[PageHeader.SizeOf] == 'X');
                    Assert.True(pagePointer[666] == 'X');
                    Assert.True(pagePointer[1039] == 'X');
                    txState.InvokeBeforeCommitFinalization(Env, ref state, ref txState);
                }
                finally
                {
                    txState.InvokeDispose(Env, ref state, ref txState);
                }
        }

        [Theory]
        [InlineDataWithRandomSeed]
        public unsafe void WriteSeekAndReadInTempCryptoStream(int seed)
        {
            using (var options = StorageEnvironmentOptions.ForPath(DataDir))
            using (var file = SafeFileStream.Create(Path.Combine(DataDir, "EncryptedTempFile"), FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None, 4096,
                       FileOptions.DeleteOnClose))
            using (var stream = new TempCryptoStream(file))
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

        [RavenMultiplatformFact(RavenTestCategory.Voron | RavenTestCategory.Encryption, RavenPlatform.Windows | RavenPlatform.Linux)]
        public unsafe void StreamsTempFile_With_Encryption_ShouldNotThrow_When_NotAllStreamsWereRead()
        {
            using (var options = StorageEnvironmentOptions.ForPath(DataDir))
            {
                options.Encryption.MasterKey = Sodium.GenerateRandomBuffer((int)Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes());
                using (var environment = new StorageEnvironment(options))
                {
                    using (var temp = new StreamsTempFile(Path.Combine(DataDir, "EncryptedTempFile"), environment))
                    {
                        for (int j = 0; j < 10; j++)
                        {
                            using (temp.Scope())
                            {
                                var streams = new List<Stream>();
                                for (var i = 0; i < 10; i++)
                                {
                                    var stream = temp.StartNewStream();
                                    var bytes = new byte[1024];
                                    fixed (byte* b = bytes)
                                    {
                                        Memory.Set(b, (byte)i, bytes.Length);
                                    }

                                    stream.Write(bytes, 0, bytes.Length);
                                    stream.Flush();
                                    streams.Add(stream);
                                }

                                streams[0].Seek(0, SeekOrigin.Begin);
                            }

                            Assert.Equal(0, temp._file.Position);
                            Assert.Equal(0, temp._file.Length);
                            Assert.True(temp._file.InnerStream.Length > 0);
                        }
                    }
                }
            }
        }

        [RavenMultiplatformFact(RavenTestCategory.Voron | RavenTestCategory.Encryption, RavenPlatform.Windows | RavenPlatform.Linux)]
        public unsafe void StreamsTempFile_With_Encryption_ShouldThrow_When_SeekAndWrite_AreMixed_Without_ExecutingReset()
        {
            using (var options = StorageEnvironmentOptions.ForPath(DataDir))
            {
                options.Encryption.MasterKey = Sodium.GenerateRandomBuffer((int)Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes());
                using (var environment = new StorageEnvironment(options))
                {
                    using (var temp = new StreamsTempFile(Path.Combine(DataDir, "EncryptedTempFile"), environment))
                    {
                        var bytes = new byte[1024];
                        fixed (byte* b = bytes)
                        {
                            Memory.Set(b, (byte)'I', bytes.Length);
                        }

                        Stream stream;
                        using (temp.Scope())
                        {
                            stream = temp.StartNewStream();

                            stream.Write(bytes, 0, bytes.Length);
                            stream.Flush();

                            stream.Seek(0, SeekOrigin.Begin);

                            var read = stream.Read(new Span<byte>(new byte[10]));
                            Assert.Equal(10, read);

                            Assert.Throws<NotSupportedException>(() => stream.Write(bytes, 0, bytes.Length));
                        }

                        Assert.Throws<NotSupportedException>(() => stream.Write(bytes, 0, bytes.Length));
                    }
                }
            }
        }

        [RavenMultiplatformFact(RavenTestCategory.Voron | RavenTestCategory.Encryption, RavenPlatform.Windows | RavenPlatform.Linux)]
        public unsafe void RavenDB_15975()
        {
            Options.Encryption.MasterKey = Sodium.GenerateRandomBuffer((int)Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes());

            var file = Env.ScratchBufferPool.GetScratchBufferFile(0);
            var (pager, state) = file.GetPagerAndState();

            Pager2.PagerTransactionState txState = new(){IsWriteTransaction = true};
            try
            {
                var overflowSize = 4 * Constants.Storage.PageSize + 100;

                pager.EnsureContinuous(ref state, 26, 5);
                var pagePointer = pager.AcquirePagePointerForNewPage(state, ref txState, 26, 5);

                var header = (PageHeader*)pagePointer;
                header->PageNumber = 26;
                header->Flags = PageFlags.Overflow;
                header->OverflowSize = overflowSize;

                Memory.Set(pagePointer + PageHeader.SizeOf, (byte)'X', overflowSize);
                txState.InvokeBeforeCommitFinalization(Env, ref state, ref txState);
            }
            finally
            {
                txState.InvokeDispose(Env, ref state, ref txState);
            }

            txState = default;
            try
                {
                    var pagePointer = pager.AcquirePagePointerWithOverflowHandling(state, ref txState, 26);

                    // Making sure that the data was decrypted and still holds those 'X' chars
                    Assert.True(pagePointer[PageHeader.SizeOf] == 'X');
                    Assert.True(pagePointer[666] == 'X');
                    Assert.True(pagePointer[1039] == 'X');
                    txState.InvokeBeforeCommitFinalization(Env, ref state, ref txState);
                }
                finally
                {
                    txState.InvokeDispose(Env, ref state, ref txState);
                }
        }

        [RavenMultiplatformFact(RavenTestCategory.Encryption | RavenTestCategory.Voron, RavenPlatform.Windows | RavenPlatform.Linux)]
        public unsafe void RavenDB_159751()
        {
            Options.Encryption.MasterKey = Sodium.GenerateRandomBuffer((int)Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes());

            var file = Env.ScratchBufferPool.GetScratchBufferFile(0);
            var (pager, state) = file.GetPagerAndState();

            Pager2.PagerTransactionState txState = new(){IsWriteTransaction = true};
            try
            {
                var overflowSize = 4 * Constants.Storage.PageSize + 100;

                pager.EnsureContinuous(ref state, 26, 5);
                var pagePointer = pager.AcquirePagePointerForNewPage(state, ref txState, 26, 5);

                var header = (PageHeader*)pagePointer;
                header->PageNumber = 26;
                header->Flags = PageFlags.Overflow;
                header->OverflowSize = overflowSize;

                Memory.Set(pagePointer + PageHeader.SizeOf, (byte)'X', overflowSize);

                Assert.True(PageExistsInCache(txState, pager, 26, out var usages));
                Assert.Equal(1, usages);

                pager.TryReleasePage(ref txState, 26);

                Assert.True(PageExistsInCache(txState, pager, 26, out usages));
                Assert.Equal(1, usages);

                pager.AcquirePagePointer(state, ref txState, 26);

                Assert.True(PageExistsInCache(txState, pager, 26, out usages));
                Assert.Equal(2, usages);
                txState.InvokeBeforeCommitFinalization(Env, ref state, ref txState);
            }
            finally
            {
                txState.InvokeDispose(Env, ref state, ref txState);
            }

            txState = default;
            try
            {
                var pagePointer = pager.AcquirePagePointerWithOverflowHandling(state, ref txState, 26);

                // Making sure that the data was decrypted and still holds those 'X' chars
                Assert.True(pagePointer[PageHeader.SizeOf] == 'X');
                Assert.True(pagePointer[666] == 'X');
                Assert.True(pagePointer[1039] == 'X');

                Assert.True(PageExistsInCache(txState, pager, 26, out var usages));
                Assert.Equal(1, usages);

                pager.AcquirePagePointerWithOverflowHandling(state, ref txState, 26);

                Assert.True(PageExistsInCache(txState, pager, 26, out usages));
                Assert.Equal(2, usages);

                pager.TryReleasePage(ref txState, 26);

                Assert.True(PageExistsInCache(txState, pager, 26, out usages));
                Assert.Equal(1, usages);

                pager.TryReleasePage(ref txState, 26);

                Assert.False(PageExistsInCache(txState, pager, 26, out usages));
                Assert.Equal(0, usages);
                txState.InvokeBeforeCommitFinalization(Env, ref state, ref txState);
            }
            finally
            {
                txState.InvokeDispose(Env, ref state, ref txState);
            }

            bool PageExistsInCache(Pager2.PagerTransactionState txState, Pager2 pager, long page, out int usages)
                {
                    if (txState.ForCrypto.TryGetValue(pager, out var s) == false)
                    {
                        usages = 0;
                        return false;
                    }

                    if (s.TryGetValue(page, out var buffer) == false)
                    {
                        usages = 0;
                        return false;
                    }

                    usages = buffer.Usages;
                    return true;
                }
        }
    }
}
