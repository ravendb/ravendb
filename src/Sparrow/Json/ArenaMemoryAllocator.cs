using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using NLog;

namespace Sparrow.Json
{
    public unsafe class ArenaMemoryAllocator : IDisposable
    {
        private byte* _ptrStart;
        private byte* _ptrCurrent;

        private int _allocated;
        private int _used;

        private List<IntPtr> _unusedBuffers;

        private bool _isDisposed;
        private static readonly Logger _log = LogManager.GetLogger(nameof(ArenaMemoryAllocator));
        
        public ArenaMemoryAllocator(int initialSize = 1024 * 1024)
        {
            _ptrStart = _ptrCurrent = (byte*)Marshal.AllocHGlobal(initialSize).ToPointer();
            _allocated = initialSize;
            _used = 0;

            if (_log.IsDebugEnabled)
                _log.Debug($"ArenaMemoryAllocator was created with initial capacity of {initialSize:#,#;;0} bytes");
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

            if (_log.IsDebugEnabled)
                _log.Debug($"ArenaMemoryAllocator allocated {size:#,#;;0} bytes");

            return allocation;
        }

        private void ThrowAlreadyDisposedException()
        {
            throw new ObjectDisposedException($"This ArenaMemoryAllocator is already disposed");
        }

        public void GrowArena(int requestedSize)
        {
            while (_used + requestedSize > _allocated)
            {
                var newSize = _allocated * 2;
                if(newSize < _allocated)
                    throw new OverflowException("Arena size overflowed");

                if (_log.IsDebugEnabled)
                    _log.Debug($"ArenaMemoryAllocator doubled size of buffer from {_allocated:#,#;0} to {newSize:#,#;0}");
                _allocated = newSize;
            }
            
            var newBuffer = (byte*) Marshal.AllocHGlobal(_allocated).ToPointer();
            
            // Save the old buffer pointer to be released at a more convenient time
            if (_unusedBuffers == null)
                _unusedBuffers = new List<IntPtr>();
            _unusedBuffers.Add(new IntPtr(_ptrStart));

            _ptrStart = newBuffer;
            _ptrCurrent = _ptrStart;
        }

        public void ResetArena()
        {
            // Reset current arena buffer
            _ptrCurrent = _ptrStart;
            _used = 0;

            // Free old buffers not being used anymore
            if (_unusedBuffers != null)
            {
                foreach (var unusedBuffer in _unusedBuffers)
                {
                    Marshal.FreeHGlobal(unusedBuffer);
                }
                _unusedBuffers = null;
            }
        }

        ~ArenaMemoryAllocator()
        {
            if (_log.IsWarnEnabled)
                _log.Warn($"ArenaMemoryAllocator wasn't properly disposed");

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