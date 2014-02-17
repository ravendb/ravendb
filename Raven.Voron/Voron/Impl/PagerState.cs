namespace Voron.Impl
{
    using System.Collections.Concurrent;
    using System.Diagnostics;
    using System.IO.MemoryMappedFiles;
    using System.Threading;
    using System.Threading.Tasks;

    using Voron.Impl.Paging;

    public unsafe class PagerState
    {
	    private readonly AbstractPager _pager;

	    public bool DisposeFilesOnDispose = true;

	    public class AllocationInfo
	    {
		    public MemoryMappedFile MappedFile;
		    public byte* BaseAddress;
		    public long Size;
	    }

#if DEBUG_PAGER_STATE
        public static ConcurrentDictionary<PagerState, StackTrace> Instances = new ConcurrentDictionary<PagerState, StackTrace>();
#endif

        public PagerState(AbstractPager pager)
        {
	        _pager = pager;
#if DEBUG_PAGER_STATE
            Instances[this] = new StackTrace(true);
#endif
		}

        private int _refs;

        public MemoryMappedViewAccessor Accessor;

        public MemoryMappedFile[] Files;

		public AllocationInfo[] AllocationInfos;

        public byte* MapBase { get; set; }

        public bool Released;

        public void Release()
        {
            if (Interlocked.Decrement(ref _refs) != 0)
                return;

#if DEBUG_PAGER_STATE
            StackTrace value;
            Instances.TryRemove(this, out value);
#endif
         
            ReleaseInternal();
            //_pager.RegisterDisposal(Task.Run(() => ReleaseInternal()));
        }

        private void ReleaseInternal()
        {
            if (Accessor != null)
            {
                Accessor.SafeMemoryMappedViewHandle.ReleasePointer();
                Accessor.Dispose();
                Accessor = null;
            }

			if (AllocationInfos != null)
			{
				foreach (var allocationInfo in AllocationInfos)
					MemoryMapNativeMethods.UnmapViewOfFile(allocationInfo.BaseAddress);
			}

			if (Files != null && DisposeFilesOnDispose)
            {
	            foreach (var file in Files)
					file.Dispose();

	            Files = null;
            }

            Released = true;
        }

#if DEBUG_PAGER_STATE
        public ConcurrentQueue<StackTrace> AddedRefs = new ConcurrentQueue<StackTrace>();
#endif

		public void AddRef()
        {
            Interlocked.Increment(ref _refs);
#if DEBUG_PAGER_STATE
			AddedRefs.Enqueue(new StackTrace(true));
			while (AddedRefs.Count > 500)
			{
				StackTrace trace;
				AddedRefs.TryDequeue(out trace);
			}
#endif
		}

        [Conditional("VALIDATE")]
        public void DebugVerify(long size)
        {
            if (AllocationInfos == null)
                return;

            foreach (var allocationInfo in AllocationInfos)
            {
                for (int i = 0; i < allocationInfo.Size; i++)
                {
                    var b = *(allocationInfo.BaseAddress + i);
                    *(allocationInfo.BaseAddress + i) = b;
                }
            }

            for (int i = 0; i < size; i++)
            {
                var b = *(MapBase + i);
                *(MapBase + i) = b;
            }
        }
    }
}