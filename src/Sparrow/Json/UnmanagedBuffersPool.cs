using System;
using System.Collections.Concurrent;
using Sparrow.Binary;
using Sparrow.Logging;
#if MEM_GUARD
using Sparrow.Platform;
#endif
using Sparrow.Utils;

namespace Sparrow.Json
{
    public unsafe class UnmanagedBuffersPool : IDisposable
    {
        protected readonly string _debugTag;

        protected readonly string _databaseName;

        private static readonly Logger _log = LoggingSource.Instance.GetLogger<UnmanagedBuffersPool>("Client");

        private readonly ConcurrentStack<AllocatedMemoryData>[] _freeSegments;

        private bool _isDisposed;

        public UnmanagedBuffersPool(string debugTag, string databaseName = null)
        {
            _debugTag = debugTag;
            _databaseName = databaseName ?? string.Empty;
            _freeSegments = new ConcurrentStack<AllocatedMemoryData>[32];
            for (int i = 0; i < _freeSegments.Length; i++)
            {
                _freeSegments[i] = new ConcurrentStack<AllocatedMemoryData>();
            }
        }

        public void LowMemory()
        {
            var size = FreeAllPooledMemory();
            if(_log.IsInfoEnabled)
                _log.Info($"{_debugTag}: HandleLowMemory freed {size:#,#;;0} bytes in {_debugTag}");
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
            if (_isDisposed == false)
            {
                if (_log.IsOperationsEnabled)
                    _log.Operations($"UnmanagedBuffersPool for {_debugTag} wasn't properly disposed");
            }

            try
            {
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

            var actualSize = Bits.NextPowerOf2(size);

            var index = GetIndexFromSize(actualSize);

            NativeMemory.ThreadStats stats;
            if (index == -1)
            {
                return new AllocatedMemoryData
                {
                    SizeInBytes = size,
                    Address = NativeMemory.AllocateMemory(size, out stats),
                    AllocatingThread = stats
                };
            }

            actualSize = GetIndexSize(ref index, actualSize); // when we request 7 bytes, we want to get 16 bytes

            if (_freeSegments[index].TryPop(out AllocatedMemoryData list))
            {
                return list;
            }
            return new AllocatedMemoryData
            {
                SizeInBytes = actualSize,
                Address = NativeMemory.AllocateMemory(actualSize, out stats),
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
            GC.SuppressFinalize(returned);
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
