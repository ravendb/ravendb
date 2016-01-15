using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using Raven.Abstractions.Logging;
using Raven.Server.Config;
using Sparrow.Binary;

namespace Raven.Server.Json
{
    public unsafe class UnmanagedBuffersPool : IDisposable
    {
        private readonly string _databaseName;

        private static readonly ILog _log = LogManager.GetLogger(typeof(UnmanagedBuffersPool));

        private readonly ConcurrentStack<AllocatedMemoryData>[] _freeSegments;

        private bool _isDisposed;

        public class AllocatedMemoryData
        {
            public IntPtr Address;
            public int SizeInBytes;
        }

        public UnmanagedBuffersPool(string databaseName)
        {
            _databaseName = databaseName;
            _freeSegments = new ConcurrentStack<AllocatedMemoryData>[16];
            for (int i = 0; i < _freeSegments.Length; i++)
            {
                _freeSegments[i] = new ConcurrentStack<AllocatedMemoryData>();
            }
        }

        // todo: add test that test concurrent handle low memory and allocations
        public void HandleLowMemory()
        {
            _log.Info("HandleLowMemory was called, will release all pooled memory for: {0}", _databaseName);
            var size = FreeAllPooledMemory();
            _log.Info("HandleLowMemory freed {1:#,#} bytes in {0}", _databaseName, size);

        }

        private long FreeAllPooledMemory()
        {
            long size = 0;
            foreach (var stack in _freeSegments)
            {
                AllocatedMemoryData allocatedMemoryDatas;
                while (stack.TryPop(out allocatedMemoryDatas))
                {
                    size += allocatedMemoryDatas.SizeInBytes;
                    Marshal.FreeHGlobal(allocatedMemoryDatas.Address);
                }
            }
            return size;
        }

        public void SoftMemoryRelease()
        {

        }

        public LowMemoryHandlerStatistics GetStats()
        {
            long size = 0;
            foreach (var stack in _freeSegments)
            {
                foreach (var allocatedMemoryData in stack)
                {
                    size += allocatedMemoryData.SizeInBytes;
                }
            }
            return new LowMemoryHandlerStatistics
            {
                DatabaseName = _databaseName,
                EstimatedUsedMemory = size,
                Name = "UnmanagedBufferPool for " + _databaseName
            };
        }

        ~UnmanagedBuffersPool()
        {
            if (_isDisposed == false)
                _log.Warn("UnmanagedBuffersPool for {0} wasn't propertly disposed", _databaseName);
            Dispose();
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
            var actualSize = Bits.NextPowerOf2(size);

            var index = GetIndexFromSize(actualSize);

            if (index == -1)
            {
                return new AllocatedMemoryData
                {
                    SizeInBytes = size,
                    Address = Marshal.AllocHGlobal(size)
                };
            }

            AllocatedMemoryData list;
            if (_freeSegments[index].TryPop(out list))
            {
                return list;
            }
            return new AllocatedMemoryData
            {
                SizeInBytes = actualSize,
                Address = Marshal.AllocHGlobal(actualSize)
            };
        }

        public static int GetIndexFromSize(int size)
        {
            Debug.Assert(size == Bits.NextPowerOf2(size));
            switch (size)
            {
                case 1:
                case 2:
                case 4:
                case 8:
                case 16:
                    return 0;
                case 32:
                    return 1;
                case 64:
                    return 2;
                case 128:
                    return 3;
                case 256:
                    return 4;
                case 512:
                    return 5;
                case 1024:
                    return 6;
                case 2048:
                case 4096:
                    return 7;
                case 8192:
                    return 8;
                case 16384:
                    return 9;
                case 32768:
                    return 10;
                case 65536:
                    return 11;
                case 131072:
                    return 12;
                case 262144:
                    return 13;
                case 524288:
                    return 14;
                case 1048576:
                    return 15;
                default:
                    return -1;// not pooled, just alloc / free as is
            }
        }

        public void Return(AllocatedMemoryData returned)
        {
            if (returned == null) throw new ArgumentNullException(nameof(returned));
            var index = GetIndexFromSize(returned.SizeInBytes);
            if (index == -1)
            {
                Marshal.FreeHGlobal(returned.Address);

                return; // strange size, just free it
            }
            _freeSegments[index].Push(returned);
        }
    }
}