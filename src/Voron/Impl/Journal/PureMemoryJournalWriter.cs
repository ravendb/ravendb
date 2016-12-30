using Sparrow;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Utils;
using Voron.Global;
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
            public long SizeIn4Kbs;
        }

        private ImmutableAppendOnlyList<Buffer> _buffers = ImmutableAppendOnlyList<Buffer>.Empty;
        private long _lastPos;

        private readonly ReaderWriterLockSlim _locker = new ReaderWriterLockSlim();

        public PureMemoryJournalWriter(StorageEnvironmentOptions options, long journalSize)
        {
            _options = options;
            NumberOfAllocated4Kb = (int) (journalSize/(4*Constants.Size.Kilobyte));
        }

        public int NumberOfAllocated4Kb { get; }
        public bool Disposed { get; private set; }
        public bool DeleteOnClose { get; set; }

        public AbstractPager CreatePager()
        {
            _locker.EnterReadLock();
            try
            {
                return new FragmentedPureMemoryPager(_options, _buffers);
            }
            finally
            {
                _locker.ExitReadLock();
            }		
        }

        public bool Read(byte* buffer, long numOfBytes, long offsetInFile)
        {
            long currentOffset = 0;
            foreach (var current in _buffers)
            {
                long offsetInPages = 0;
                if (currentOffset != offsetInFile)
                {
                    var chunkSize = current.SizeIn4Kbs * 4 * Constants.Size.Kilobyte;
                    if (currentOffset + chunkSize <= offsetInFile)
                    {
                        currentOffset += chunkSize;
                        continue;
                    }
                    offsetInPages = offsetInFile - currentOffset;
                }

                var bytesAvailableToRead = (current.SizeIn4Kbs * 4 * Constants.Size.Kilobyte - offsetInPages);
                var actualCount = Math.Min(numOfBytes, bytesAvailableToRead);

                Memory.Copy(buffer, current.Pointer + (offsetInPages * _options.PageSize), actualCount);
                buffer += actualCount;
                numOfBytes -= actualCount;
                offsetInFile += bytesAvailableToRead;
                currentOffset += bytesAvailableToRead;
                if (numOfBytes <= 0)
                    return true;
            }
            return false;
        }

        public void Truncate(long size)
        {
            // nothing to do here
        }

        public void Dispose()
        {
            Disposed = true;
            foreach (var buffer in _buffers)
            {
                NativeMemory.Free(buffer.Pointer, buffer.SizeIn4Kbs*Constants.Storage.PageSize);
            }
            _buffers = ImmutableAppendOnlyList<Buffer>.Empty;
        }

        public void Write(long position, byte* p, int numberOf4Kb)
        {
            _locker.EnterWriteLock();
            try
            {
                if (position != _lastPos)
                    throw new InvalidOperationException("Journal writes must be to the next location in the journal");

                var size = numberOf4Kb*4*Constants.Size.Kilobyte;
                _lastPos += numberOf4Kb;

                var handle = NativeMemory.AllocateMemory(size);

                var buffer = new Buffer
                {
                    Pointer = handle,
                    SizeIn4Kbs = numberOf4Kb
                };
                _buffers = _buffers.Append(buffer);

                Memory.Copy(buffer.Pointer, p, numberOf4Kb * 4 * Constants.Size.Kilobyte);
            }
            finally
            {
                _locker.ExitWriteLock();
            }
        }
    }
}
