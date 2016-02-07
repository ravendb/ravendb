using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Raven.Abstractions.Logging;
using Raven.Server.Config;
using Sparrow.Binary;

namespace Raven.Server.Json
{
    public unsafe class UnmanagedBuffersPool : IDisposable
    {
        private readonly string _debugTag;

        private static readonly ILog _log = LogManager.GetLogger(typeof(UnmanagedBuffersPool));

        private readonly ConcurrentStack<AllocatedMemoryData>[] _freeSegments;

        private bool _isDisposed;

        public class AllocatedMemoryData
        {
            public IntPtr Address;
            public int SizeInBytes;
        }

        public UnmanagedBuffersPool(string debugTag)
        {
            _debugTag = debugTag;
            _freeSegments = new ConcurrentStack<AllocatedMemoryData>[32];
            for (int i = 0; i < _freeSegments.Length; i++)
            {
                _freeSegments[i] = new ConcurrentStack<AllocatedMemoryData>();
            }
        }

        // todo: add test that test concurrent handle low memory and allocations
        public void HandleLowMemory()
        {
            _log.Info("HandleLowMemory was called, will release all pooled memory for: {0}", _debugTag);
            var size = FreeAllPooledMemory();
            _log.Info("HandleLowMemory freed {1:#,#} bytes in {0}", _debugTag, size);

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
                DatabaseName = _debugTag,
                EstimatedUsedMemory = size,
                Name = "UnmanagedBufferPool for " + _debugTag
            };
        }

        ~UnmanagedBuffersPool()
        {
            if (_isDisposed == false)
                _log.Warn("UnmanagedBuffersPool for {0} wasn't propertly disposed", _debugTag);
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
            actualSize = GetIndexSize(index, actualSize); // when we request 7 bytes, we want to get 16 bytes
            return new AllocatedMemoryData
            {
                SizeInBytes = actualSize,
                Address = Marshal.AllocHGlobal(actualSize)
            };
        }


        private static int GetIndexSize(int index, int powerBy2Size)
        {
            switch (index)
            {
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                    return 16;
                case 12:
                case 13:
                    return 4096;
                default:
                    return powerBy2Size;
            }
        }

        public static int GetIndexFromSize(int size)
        {
            if (size > 1024*1024)
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