using Sparrow.Utils;
using Voron;
using Voron.Global;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Voron.Impl.Scratch;
using Voron.Util;
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

            var buffer = new PureMemoryJournalWriter.Buffer
            {
                Pointer = handle,
                SizeInPages = numberOfPages
            };

            try
            {
                using (var pager = new FragmentedPureMemoryPager(StorageEnvironmentOptions.CreateMemoryOnly(), ImmutableAppendOnlyList<PureMemoryJournalWriter.Buffer>.Empty.Append(buffer)))
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
            finally
            {
                NativeMemory.Free(handle, buffer.SizeInPages);
            }
            
        }
    }
}