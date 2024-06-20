using System;
using System.Threading;
using FastTests.Voron;
using Sparrow.Platform;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Issues
{
    public class RavenDB_17054 : StorageTest
    {
        public RavenDB_17054(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.Encryption.MasterKey = Sodium.GenerateRandomBuffer((int)Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes());
            options.MaxScratchBufferSize = 65536 - 1; // to make ShouldReduceSizeOfCompressionPager() return true
            options.Encryption.RegisterForJournalCompressionHandler();
        }

        [Fact]
        public void WeNeedToTakeWriteLockWhenZeroCompressionBufferIsCalled()
        {
            var testingActionWasCalled = false;

            Exception startTransactionException = null;

            var zeroCompressionBufferThread = new Thread(() =>
            {
                try
                {
                    using (var tx = Env.WriteTransaction())
                    {
                        Env.Journal.ZeroCompressionBuffer(ref tx.LowLevelTransaction.PagerTransactionState); // this can be called by an index e.g. when forceMemoryCleanup is true 
                    }
                }
                catch (Exception e)
                {
                    startTransactionException = e;
                }
            });

            Env.Journal.ForTestingPurposesOnly().OnReduceSizeOfCompressionBufferIfNeeded_RightAfterDisposingCompressionPager += () =>
            {
                testingActionWasCalled = true;

                zeroCompressionBufferThread.Start();

                Thread.Sleep(1000); // give the thread more time to ensure that ZeroCompressionBuffer will wait for _writeLock
            };

            // in RavenDB we call TryReduceSizeOfCompressionBufferIfNeeded when a database is idle, then we call IndexStore?.RunIdleOperations(mode)
            // although an index can still run ZeroCompressionBuffer concurrently e.g. when forceMemoryCleanup is true
            Env.Journal.TryReduceSizeOfCompressionBufferIfNeeded();

            Assert.True(testingActionWasCalled);

            Assert.True(zeroCompressionBufferThread.Join(TimeSpan.FromSeconds(30)), "zeroCompressionBufferThread.Join(TimeSpan.FromSeconds(30))");

            Assert.Null(startTransactionException);
        }
    }
}
