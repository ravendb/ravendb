using System;
using System.Collections.Concurrent;
using Sparrow.Binary;
using Sparrow.Logging;
using Sparrow.LowMemory;
#if MEM_GUARD
using Sparrow.Platform;
#endif
using Sparrow.Utils;
using static Sparrow.DisposableExceptions;

namespace Sparrow.Json
{
    public unsafe class UnmanagedBuffersPool : IDisposable, IDisposableQueryable
    {
        private readonly IRavenLogger _logger;

        protected readonly string _debugTag;

        protected readonly string _databaseName;

        private readonly ConcurrentStack<AllocatedMemoryData>[] _freeSegments;

        private bool _isDisposed;
        bool IDisposableQueryable.IsDisposed => _isDisposed;

        public UnmanagedBuffersPool(IRavenLogger logger, string debugTag, string databaseName = null)
        {
            _logger = logger;
            _debugTag = debugTag;
            _databaseName = databaseName ?? string.Empty;
            _freeSegments = new ConcurrentStack<AllocatedMemoryData>[32];
            for (int i = 0; i < _freeSegments.Length; i++)
            {
                _freeSegments[i] = new ConcurrentStack<AllocatedMemoryData>();
            }
        }

        public void LowMemory(LowMemorySeverity lowMemorySeverity)
        {
            if (lowMemorySeverity != LowMemorySeverity.ExtremelyLow)
                return;

            var size = FreeAllPooledMemory();
            if (_logger.IsDebugEnabled && size > 0)
                _logger.Debug($"{_debugTag}: HandleLowMemory freed {new Size(size, SizeUnit.Bytes)} in {_debugTag}");
        }

        private long FreeAllPooledMemory()
        {
            long size = 0;

            foreach (var stack in _freeSegments)
            {
                while (stack.TryPop(out var allocatedMemoryDatas))
                {
                    size += allocatedMemoryDatas.SizeInBytes;
                    NativeMemory.Free(allocatedMemoryDatas.Address, allocatedMemoryDatas.SizeInBytes, allocatedMemoryDatas.AllocatingThread);
#if MEM_GUARD
#if MEM_GUARD_STACK
                    allocatedMemoryDatas.FreedBy = Environment.StackTrace;
#endif
                    GC.SuppressFinalize(allocatedMemoryDatas);
#endif
                }
            }

            return size;
        }

        public void LowMemoryOver()
        {
        }

        public long GetAllocatedMemorySize()
        {
            long size = 0;
            foreach (var stack in _freeSegments)
            {
                foreach (var allocatedMemoryData in stack)
                {
                    size += allocatedMemoryData.SizeInBytes;
                }
            }
            return size;
        }

        ~UnmanagedBuffersPool()
        {
            // There is no reason why we want to pay try..catch overhead where the object has been already been disposed.
            if (_isDisposed)
                return;

            try
            {
                if (_logger.IsErrorEnabled)
                    _logger.Error($"UnmanagedBuffersPool for {_debugTag} wasn't properly disposed");

                Dispose();
            }
            catch (ObjectDisposedException)
            {
                // This is expected, we might be calling the finalizer on an object that
                // was already disposed, we don't want to error here because of this
            }
        }

        public void Dispose()
        {
            // We may not want to execute `.Dispose()` more than once in release, but we want to fail fast in debug if it happens.
            ThrowIfDisposedOnDebug(this);
            
            if (_isDisposed)
                return;

            FreeAllPooledMemory();

            _isDisposed = true;
            GC.SuppressFinalize(this);
        }

        public AllocatedMemoryData Allocate(int size)
        {
#if MEM_GUARD
            return new AllocatedMemoryData
            {
                SizeInBytes = size,
                Address = ElectricFencedMemory.Allocate(size),
            };
#else

            var actualSize = Bits.PowerOf2(size);

            var index = GetIndexFromSize(actualSize);

            NativeMemory.ThreadStats stats;
            if (index == -1)
            {
                return new AllocatedMemoryData(NativeMemory.AllocateMemory(size, out stats), size)
                {
                    AllocatingThread = stats
                };
            }

            actualSize = GetIndexSize(ref index, actualSize); // when we request 7 bytes, we want to get 16 bytes

            if (_freeSegments[index].TryPop(out AllocatedMemoryData list))
            {
                return list;
            }
            return new AllocatedMemoryData(NativeMemory.AllocateMemory(actualSize, out stats), actualSize)
            {
                AllocatingThread = stats
            };
#endif
        }


        private static int GetIndexSize(ref int index, int powerBy2Size)
        {
            switch (index)
            {
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                    index = 5;
                    return 16;
                case 12:
                case 13:
                    index = 13;
                    return 4096;
                default:
                    return powerBy2Size;
            }
        }

        public static int GetIndexFromSize(int size)
        {
            if (size > 1024 * 1024)
                return -1;

            var c = 0;
            while (size > 0)
            {
                size >>= 1;
                c++;
            }
            return c;
        }

        public void Return(AllocatedMemoryData returned)
        {
#if MEM_GUARD
            ElectricFencedMemory.Free(returned.Address);
#else

            if (returned == null) throw new ArgumentNullException(nameof(returned));
            var index = GetIndexFromSize(returned.SizeInBytes);
            if (index == -1)
            {
                NativeMemory.Free(returned.Address, returned.SizeInBytes, returned.AllocatingThread);

                return; // strange size, just free it
            }
            _freeSegments[index].Push(returned);
#endif

        }
    }
}
