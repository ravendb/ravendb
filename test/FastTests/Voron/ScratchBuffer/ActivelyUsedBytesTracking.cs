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
                    
                    var p0 = file.Allocate(tx.LowLevelTransaction, 1, 1, 8, default);
                    var p1 = file.Allocate(tx.LowLevelTransaction, 1, 1, 9, default);
                    var p2 = file.Allocate(tx.LowLevelTransaction, 1, 1, 10, default);
                    var p3 = file.Allocate(tx.LowLevelTransaction, 1, 1, 11, default);
                    var p4 = file.Allocate(tx.LowLevelTransaction, 1, 1, 12, default);

                    file.Free(tx.LowLevelTransaction, 1, p0.PositionInScratchBuffer);
                    file.Free(tx.LowLevelTransaction, 3, p1.PositionInScratchBuffer);
                    file.Free(tx.LowLevelTransaction, 4, p2.PositionInScratchBuffer);
                    file.Free(tx.LowLevelTransaction, 7, p3.PositionInScratchBuffer);
                    file.Free(tx.LowLevelTransaction, 9, p4.PositionInScratchBuffer);

                    for (int i = 0; i <= 9; i++)
                    {
                        bool hasActivelyUsedBytes = file.HasActivelyUsedBytes(i);
                        Assert.True(hasActivelyUsedBytes, "hasActivelyUsedBytes on " + i);
                    }

                    Assert.False(file.HasActivelyUsedBytes(10));
                    Assert.False(file.HasActivelyUsedBytes(20));
                }
            }
        }
    }
}
