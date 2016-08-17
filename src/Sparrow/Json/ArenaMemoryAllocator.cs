using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Sparrow.Logging;

namespace Sparrow.Json
{
    public unsafe class ArenaMemoryAllocator : IDisposable
    {
        private byte* _ptrStart;
        private byte* _ptrCurrent;

        private int _allocated;
        private int _used;

        private List<IntPtr> _olderBuffers;

        private bool _isDisposed;
        private static readonly Logger _logger = LoggerSetup.Instance.GetLogger<ArenaMemoryAllocator>("ArenaMemoryAllocator");

        public int Allocated => _allocated;

        public ArenaMemoryAllocator(int initialSize = 1024 * 1024)
        {
            _ptrStart = _ptrCurrent = (byte*)Marshal.AllocHGlobal(initialSize).ToPointer();
            _allocated = initialSize;
            _used = 0;

            if (_logger.IsInfoEnabled)
                _logger.Info($"ArenaMemoryAllocator was created with initial capacity of {initialSize:#,#;;0} bytes");
        }

        public AllocatedMemoryData Allocate(int size)
        {
            if (_isDisposed)
                ThrowAlreadyDisposedException();

            if (_used + size > _allocated)
                GrowArena(size);

            var allocation = new AllocatedMemoryData()
            {
                SizeInBytes = size,
                Address = new IntPtr(_ptrCurrent)
            };

            _ptrCurrent += size;
            _used += size;

            if (_logger.IsInfoEnabled)
                _logger.Info($"ArenaMemoryAllocator allocated {size:#,#;;0} bytes");

            return allocation;
        }

        private void ThrowAlreadyDisposedException()
        {
            throw new ObjectDisposedException("This ArenaMemoryAllocator is already disposed");
        }

        public void GrowArena(int requestedSize)
        {
            if (requestedSize >= 1024 * 1024 * 1024)
                throw new ArgumentOutOfRangeException(nameof(requestedSize));

            int newSize=0;
            do
            {
                newSize = _allocated * 2;
                if (newSize < _allocated) // Overflow
                {
                    // int.MaxValue = 2147483647 which is not a power of 2. The largest power of 2 contained in a signed int is 1GB.
                    // Since we want to allocate in blocks of powers of 2 the max buffer size will be 1GB
                    newSize = _allocated;
                    if (_logger.IsInfoEnabled)
                        _logger.Info("Arena main buffer reached maximum size of 1GB, check if you forgot to reset the context. From now on we grow this arena in 1GB chunks.");
                }
            } while (requestedSize > newSize);
            
            if (_logger.IsInfoEnabled)
                _logger.Info($"Increased size of buffer from {_allocated:#,#;0} to {newSize:#,#;0} because we need {requestedSize:#,#;0}. _used={_used:#,#;0}");
            
            var newBuffer = (byte*) Marshal.AllocHGlobal(newSize).ToPointer();
            _allocated = newSize;

            // Save the old buffer pointer to be released when the arena is reset
            if (_olderBuffers == null)
                _olderBuffers = new List<IntPtr>();
            _olderBuffers.Add(new IntPtr(_ptrStart));

            _ptrStart = newBuffer;
            _ptrCurrent = _ptrStart;
            _used = 0;
        }

        public void ResetArena()
        {
            // Reset current arena buffer
            _ptrCurrent = _ptrStart;
            _used = 0;

            // Free old buffers not being used anymore
            if (_olderBuffers != null)
            {
                foreach (var unusedBuffer in _olderBuffers)
                {
                    Marshal.FreeHGlobal(unusedBuffer);
                }
                _olderBuffers = null;
            }
        }

        ~ArenaMemoryAllocator()
        {
            if (_logger.IsInfoEnabled)
                _logger.Info("ArenaMemoryAllocator wasn't properly disposed");

            Dispose();
        }

        public void Dispose()
        {
            if (_isDisposed)
                return;
            _isDisposed = true;

            ResetArena();

            Marshal.FreeHGlobal(new IntPtr(_ptrStart));

            GC.SuppressFinalize(this);
        }
    }

    public class AllocatedMemoryData
    {
        public IntPtr Address;
        public int SizeInBytes;
    }
}