using Sparrow.Utils;
using Voron;
using Voron.Global;
using Voron.Impl.Paging;
using Voron.Impl.Scratch;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron.ScratchBuffer
{
    public class ActivelyUsedBytesTracking : NoDisposalNeeded
    {
        public ActivelyUsedBytesTracking(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public unsafe void Scratch_file_is_aware_of_potentialy_active_readers()
        {
            var numberOfPages = 20;
            var handle = NativeMemory.AllocateMemory(numberOfPages * Constants.Storage.PageSize);


            using (var options = StorageEnvironmentOptions.CreateMemoryOnly())
            using(var env = new StorageEnvironment(options))
            {
                var (pager, state) = options.CreateScratchPager("temp", 65*1024);
                using var _ = pager;
                using (var file = new ScratchBufferFile(pager, state, 0))
                using(var tx = env.WriteTransaction())
                {
                    Assert.False(file.HasActivelyUsedBytes(2));
                    
                    file.Allocate(tx.LowLevelTransaction, 1, 1, 8);
                    file.Allocate(tx.LowLevelTransaction, 1, 1, 9);
                    file.Allocate(tx.LowLevelTransaction, 1, 1, 10);
                    file.Allocate(tx.LowLevelTransaction, 1, 1, 11);
                    file.Allocate(tx.LowLevelTransaction, 1, 1, 12);

                    file.Free(tx.LowLevelTransaction, 0);
                    file.Free(tx.LowLevelTransaction, 1);
                    file.Free(tx.LowLevelTransaction, 2);
                    file.Free(tx.LowLevelTransaction, 3);
                    file.Free(tx.LowLevelTransaction, 4);

                    for (int i = 0; i <= 9; i++)
                    {
                        Assert.True(file.HasActivelyUsedBytes(i));
                    }

                    Assert.False(file.HasActivelyUsedBytes(10));
                    Assert.False(file.HasActivelyUsedBytes(20));
                }
            }
        }
    }
}
