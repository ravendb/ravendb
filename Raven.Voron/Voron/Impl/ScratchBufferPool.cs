using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Voron.Exceptions;
using Voron.Impl.Paging;
using Voron.Trees;
using Voron.Util;

namespace Voron.Impl
{
    /// <summary>
    /// This class implements the page pool for in flight transaction information
    /// Pages allocated from here are expected to live after the write transaction that 
    /// created them. The pages will be kept around until the flush for the journals
    /// send them to the data file.
    /// 
    /// This class relies on external syncronization and is not meant to be used in multiple
    /// threads at the same time
    /// </summary>
    public unsafe class ScratchBufferPool : IDisposable
    {
        private readonly IVirtualPager _scratchPager;
        private readonly Dictionary<long, LinkedList<PendingPage>> _freePagesBySize = new Dictionary<long, LinkedList<PendingPage>>();
        private readonly Dictionary<long, PageFromScratchBuffer> _allocatedPages = new Dictionary<long, PageFromScratchBuffer>();
        private long _lastUsedPage;
	    private long _sizeLimit;

        private class PendingPage
        {
            public long Page;
            public long ValidAfterTransactionId;
        }

        public ScratchBufferPool(StorageEnvironment env)
        {
            _scratchPager = env.Options.CreateScratchPager("scratch.buffers");
            _scratchPager.AllocateMorePages(null, env.Options.InitialFileSize.HasValue ? Math.Max(env.Options.InitialFileSize.Value, env.Options.InitialLogFileSize) : env.Options.InitialLogFileSize);
	        _sizeLimit = env.Options.MaxScratchBufferSize;
        }

        public PagerState PagerState { get { return _scratchPager.PagerState; } }

        public PageFromScratchBuffer Allocate(Transaction tx, int numberOfPages)
        {
            var size = Utils.NearestPowerOfTwo(numberOfPages);

            PageFromScratchBuffer result;
            if (TryGettingFromAllocatedBuffer(tx, numberOfPages, size, out result))
                return result;

	        if ((_lastUsedPage + size)*AbstractPager.PageSize > _sizeLimit)
	        {
		        throw new ScratchBufferSizeLimitException(string.Format("Cannot allocate more space for the scratch buffer. Its current size is {0} bytes. The limit is {1} bytes, while one tried to increase the size to {2} bytes.", _scratchPager.NumberOfAllocatedPages*AbstractPager.PageSize, _sizeLimit, (_lastUsedPage + size)*AbstractPager.PageSize));
	        }

            // we don't have free pages to give out, need to allocate some
            _scratchPager.EnsureContinuous(tx, _lastUsedPage, (int)size);

            result = new PageFromScratchBuffer
            {
                PositionInScratchBuffer = _lastUsedPage,
                Size = size,
                NumberOfPages = numberOfPages
            };

            _allocatedPages.Add(_lastUsedPage, result);
            _lastUsedPage += size;

            return result;
        }

        private bool TryGettingFromAllocatedBuffer(Transaction tx, int numberOfPages, long size, out PageFromScratchBuffer result)
        {
            result = null;
            LinkedList<PendingPage> list;
            if (!_freePagesBySize.TryGetValue(size, out list) || list.Count <= 0)
                return false;
            var val = list.Last.Value;
            if (val.ValidAfterTransactionId >= tx.Environment.OldestTransaction)
                return false;

            list.RemoveLast();
            var pageFromScratchBuffer = new PageFromScratchBuffer
            {
                PositionInScratchBuffer = val.Page,
                Size = size,
                NumberOfPages = numberOfPages
            };

			_allocatedPages.Add(val.Page, pageFromScratchBuffer);
            {
                result = pageFromScratchBuffer;
				return true;
            }
        }

        public void Free(long page, long asOfTxId)
        {
            PageFromScratchBuffer value;
			if (_allocatedPages.TryGetValue(page, out value) == false)
	        {
		        throw new InvalidOperationException("Attempt to free page that wasn't currently allocated: " + page);
	        }
	        _allocatedPages.Remove(page);
            LinkedList<PendingPage> list;
            if (_freePagesBySize.TryGetValue(value.Size, out list) == false)
            {
                list = new LinkedList<PendingPage>();
                _freePagesBySize[value.Size] = list;
            }
            list.AddFirst(new PendingPage
            {
                Page = value.PositionInScratchBuffer,
                ValidAfterTransactionId = asOfTxId
            });
        }

        public void Dispose()
        {
            _scratchPager.Dispose();
        }

        public Page ReadPage(long p, PagerState pagerState = null)
        {
            return _scratchPager.Read(p, pagerState);
        }

        public byte* AcquirePagePointer(long p)
        {
            return _scratchPager.AcquirePagePointer(p);
        }
    }

    public class PageFromScratchBuffer
    {
        public long PositionInScratchBuffer;
        public long Size;
        public int NumberOfPages;

	    public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;

            var other = (PageFromScratchBuffer)obj;

            return PositionInScratchBuffer == other.PositionInScratchBuffer && Size == other.Size && NumberOfPages == other.NumberOfPages;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = PositionInScratchBuffer.GetHashCode();
                hashCode = (hashCode * 397) ^ Size.GetHashCode();
                hashCode = (hashCode * 397) ^ NumberOfPages;
                return hashCode;
            }
        }

	    public override string ToString()
	    {
		    return string.Format("PositionInScratchBuffer: {0}, Size: {1}, NumberOfPages: {2}", PositionInScratchBuffer, Size, NumberOfPages);
	    }
    }
}