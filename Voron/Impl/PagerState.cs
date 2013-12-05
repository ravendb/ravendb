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

#if DEBUG
        public static ConcurrentDictionary<PagerState, StackTrace> Instances = new ConcurrentDictionary<PagerState, StackTrace>();
#endif

        public PagerState(AbstractPager pager)
        {
	        _pager = pager;
#if DEBUG
            Instances[this] = new StackTrace(true);
#endif
        }

        private int _refs;

        public MemoryMappedViewAccessor Accessor;

        public MemoryMappedFile File;

        public byte* MapBase { get; set; }

        public bool Released;

        public void Release()
        {
            if (Interlocked.Decrement(ref _refs) != 0)
                return;

#if DEBUG
            StackTrace value;
            Instances.TryRemove(this, out value);
#endif

	        _pager.RegisterDisposal(Task.Run(() => ReleaseInternal()));
        }

        private void ReleaseInternal()
        {
            if (Accessor != null)
            {
                Accessor.SafeMemoryMappedViewHandle.ReleasePointer();
                Accessor.Dispose();
                Accessor = null;
            }

            if (File != null)
            {
                File.Dispose();
                File = null;
            }

            Released = true;
        }

#if DEBUG
        public ConcurrentQueue<StackTrace> AddedRefs = new ConcurrentQueue<StackTrace>();
#endif

        public void AddRef()
        {
            Interlocked.Increment(ref _refs);
#if DEBUG
            AddedRefs.Enqueue(new StackTrace(true));
	        while (AddedRefs.Count > 500)
	        {
		        StackTrace trace;
		        AddedRefs.TryDequeue(out trace);
	        }
#endif
        }
    }
}