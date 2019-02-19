using Sparrow.Utils;
using Voron;
using Voron.Global;
using Voron.Impl.Scratch;
using Xunit;

namespace FastTests.Voron.ScratchBuffer
{
    public class ActivelyUsedBytesTracking : NoDisposalNeeded
    {
        [Fact]
        public unsafe void Scratch_file_is_aware_of_potentialy_active_readers()
        {
            var numberOfPages = 20;
            var handle = NativeMemory.AllocateMemory(numberOfPages * Constants.Storage.PageSize);


            using (var env = StorageEnvironmentOptions.CreateMemoryOnly())
            using (var pager = env.CreateScratchPager("temp", 65*1024))
            using (var file = new ScratchBufferFile(pager, 0))
            {
                Assert.False(file.HasActivelyUsedBytes(2));

                file.Allocate(null, 1, 1);
                file.Allocate(null, 1, 1);
                file.Allocate(null, 1, 1);
                file.Allocate(null, 1, 1);
                file.Allocate(null, 1, 1);

                file.Free(0, 1);
                file.Free(1, 3);
                file.Free(2, 4);
                file.Free(3, 7);
                file.Free(4, 9);

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