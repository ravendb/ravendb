using Sparrow;
using System;
using System.Runtime.InteropServices;
using System.Threading;
using Voron.Impl.Paging;
using Voron.Util;

namespace Voron.Impl.Journal
{
    public unsafe class PureMemoryJournalWriter : IJournalWriter
    {
        internal class Buffer
        {
            public byte* Pointer;
            public long SizeInPages;
            public IntPtr Handle;
        }

        private ImmutableAppendOnlyList<Buffer> _buffers = ImmutableAppendOnlyList<Buffer>.Empty;
        private long _lastPos;

        private readonly ReaderWriterLockSlim _locker = new ReaderWriterLockSlim();

        public PureMemoryJournalWriter(long journalSize)
        {
            NumberOfAllocatedPages = journalSize/AbstractPager.PageSize;
        }

        public long NumberOfAllocatedPages { get; private set; }
        public bool Disposed { get; private set; }
        public bool DeleteOnClose { get; set; }

        public IVirtualPager CreatePager()
        {
            _locker.EnterReadLock();
            try
            {
                return new FragmentedPureMemoryPager(_buffers);
            }
            finally
            {
                _locker.ExitReadLock();
            }		
        }
        
        public bool Read(long pageNumber, byte* buffer, int count)
        {
            long pos = 0;

            foreach (var current in _buffers)
            {
                if (pos < pageNumber)
                {
                    pos += current.SizeInPages;
                    continue;
                }

                var toRead = Math.Min(count, (int)current.SizeInPages * AbstractPager.PageSize);

                Memory.Copy(buffer, current.Pointer, toRead);

                count -= toRead;

                if (count == 0)
                    return true;
            }

            return false;
        }

        public void Dispose()
        {
            Disposed = true;
            foreach (var buffer in _buffers)
            {
                Marshal.FreeHGlobal(buffer.Handle);
            }
            _buffers = ImmutableAppendOnlyList<Buffer>.Empty;
        }

        public void WriteGather(long position, IntPtr[] pages)
        {
            _locker.EnterWriteLock();
            try
            {
                if (position != _lastPos)
                    throw new InvalidOperationException("Journal writes must be to the next location in the journal");

                var size = pages.Length * AbstractPager.PageSize;
                _lastPos += size;

                var handle = Marshal.AllocHGlobal(size);

                var buffer = new Buffer
                {
                    Handle = handle,
                    Pointer = (byte*)handle.ToPointer(),
                    SizeInPages = pages.Length
                };
                _buffers = _buffers.Append(buffer);

                for (int index = 0; index < pages.Length; index++)
                {
                    Memory.Copy(buffer.Pointer + (index * AbstractPager.PageSize), (byte*)pages[index].ToPointer(), AbstractPager.PageSize);
                }
            }
            finally
            {
                _locker.ExitWriteLock();
            }
        }
    }
}
