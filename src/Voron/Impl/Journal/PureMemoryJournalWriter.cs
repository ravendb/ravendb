using Sparrow;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Voron.Impl.Paging;
using Voron.Util;

namespace Voron.Impl.Journal
{
    public unsafe class PureMemoryJournalWriter : IJournalWriter
    {
        private readonly StorageEnvironmentOptions _options;

        internal class Buffer
        {
            public byte* Pointer;
            public long SizeInPages;
            public IntPtr Handle;
        }

        private ImmutableAppendOnlyList<Buffer> _buffers = ImmutableAppendOnlyList<Buffer>.Empty;
        private long _lastPos;

        private readonly ReaderWriterLockSlim _locker = new ReaderWriterLockSlim();

        public PureMemoryJournalWriter(StorageEnvironmentOptions options, long journalSize)
        {
            _options = options;
            NumberOfAllocatedPages = journalSize/options.PageSize;
        }

        public long NumberOfAllocatedPages { get; private set; }
        public bool Disposed { get; private set; }
        public bool DeleteOnClose { get; set; }

        public IVirtualPager CreatePager()
        {
            _locker.EnterReadLock();
            try
            {
                return new FragmentedPureMemoryPager(_options.PageSize, _buffers);
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
                if (pos != pageNumber)
                {
                    pos += current.SizeInPages;
                    
                    continue;
                }
               
                var actualCount = Math.Min(count, (int)(current.SizeInPages*_options.PageSize));
               
                Memory.Copy(buffer, current.Pointer, actualCount);
                count -= actualCount;
                pageNumber += current.SizeInPages;
                if (count <= 0)
                    return true;
            }
            return false;
        }

        public unsafe void WriteBuffer(long position, byte* srcPointer, int sizeToWrite)
        {
            _locker.EnterWriteLock();
            try
            {
                if (position != _lastPos)
                    throw new InvalidOperationException("Journal writes must be to the next location in the journal");

                _lastPos += sizeToWrite;

                var handle = Marshal.AllocHGlobal(sizeToWrite);

                var buffer = new Buffer
                {
                    Handle = handle,
                    Pointer = (byte*)handle.ToPointer(),
                    SizeInPages = sizeToWrite / _options.PageSize
                };
                _buffers = _buffers.Append(buffer);

                Memory.Copy(buffer.Pointer, (byte*)srcPointer, sizeToWrite);
            }
            finally
            {
                _locker.ExitWriteLock();
            }
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

                var size = pages.Length * _options.PageSize;
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
                    Memory.Copy(buffer.Pointer + (index * _options.PageSize), (byte*)pages[index].ToPointer(), _options.PageSize);
                }
            }
            finally
            {
                _locker.ExitWriteLock();
            }
        }
    }
}
