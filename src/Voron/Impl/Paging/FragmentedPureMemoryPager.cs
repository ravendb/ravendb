using System;
using System.Collections.Generic;
using System.Linq;
using Voron.Data.BTrees;
using Voron.Impl.Journal;
using Voron.Util;

namespace Voron.Impl.Paging
{
    public class FragmentedPureMemoryPager : AbstractPager
    {
        private readonly ImmutableAppendOnlyList<PureMemoryJournalWriter.Buffer> _buffers;


        internal FragmentedPureMemoryPager(StorageEnvironmentOptions options, ImmutableAppendOnlyList<PureMemoryJournalWriter.Buffer> buffers)
            : base(options)
        {
            _buffers = buffers;
            NumberOfAllocatedPages = buffers.Sum(x => x.SizeInPages);
        }

        protected override string GetSourceName()
        {
            return "FragmentedPureMemoryPager";
        }

        public override unsafe byte* AcquirePagePointer(LowLevelTransaction tx, long pageNumber, PagerState pagerState = null)
        {
            long page = 0;
            foreach (var buffer in _buffers)
            {
                if (page + buffer.SizeInPages > pageNumber)
                    return buffer.Pointer + ((pageNumber - page)* _pageSize);

                page += buffer.SizeInPages;
            }
            throw new InvalidOperationException("Could not find a matching page number: " + pageNumber);
        }

        protected override PagerState AllocateMorePages(long newLength)
        {
            throw new NotSupportedException();
        }

        public override void Sync()
        {
            throw new NotSupportedException();
        }


        public override string ToString()
        {
            return "memory";
        }

        public override unsafe void ReleaseAllocationInfo(byte* baseAddress, long size)
        {
            throw new NotSupportedException();
        }

        public override void TryPrefetchingWholeFile()
        {
        }

        public override void MaybePrefetchMemory(List<long> pagesToPrefetch)
        {
        }

        public override void MaybePrefetchMemory(List<TreePage> sortedPagesToWrite)
        {
        }
    }
}
