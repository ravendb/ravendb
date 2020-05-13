using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json.Parsing;
using Sparrow.LowMemory;
using Sparrow.Server;
using Sparrow.Server.Platform;
using Sparrow.Utils;
using Constants = Voron.Global.Constants;

namespace Voron.Impl
{
    public unsafe class EncryptionBuffersPool : ILowMemoryHandler
    {
        public static EncryptionBuffersPool Instance = new EncryptionBuffersPool();

        private readonly long _maxBufferSizeToKeepInBytes = new Size(8, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes);

        private class NativeAllocation
        {
            public IntPtr Ptr;
            public long Size;
        }

        private readonly ConcurrentStack<NativeAllocation>[] _items;

        public EncryptionBuffersPool()
        {
            var numberOfSlots = Bits.MostSignificantBit(_maxBufferSizeToKeepInBytes) + 1;
            _items = new ConcurrentStack<NativeAllocation>[numberOfSlots];

            for (int i = 0; i < _items.Length; i++)
            {
                _items[i] = new ConcurrentStack<NativeAllocation>();
            }

            LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);
        }

        public byte* Get(int size, out NativeMemory.ThreadStats thread)
        {
            size = Bits.PowerOf2(size);

            if (size > Constants.Size.Megabyte * 16)
            {
                // We don't want to pool large buffers
                return PlatformSpecific.NativeMemory.Allocate4KbAlignedMemory(size, out thread);
            }

            var index = Bits.MostSignificantBit(size);

            if (_items[index].TryPop(out var allocation))
            {
                thread = NativeMemory.ThreadAllocations.Value;
                thread.Allocations += size;
                return (byte*)allocation.Ptr;
            }

            return PlatformSpecific.NativeMemory.Allocate4KbAlignedMemory(size, out thread);
        }

        public void Return(byte* ptr, long size, NativeMemory.ThreadStats allocatingThread)
        {
            if (ptr == null)
                return;

            size = Bits.PowerOf2(size);
            Sodium.sodium_memzero(ptr, (UIntPtr)size);

            if (size > _maxBufferSizeToKeepInBytes || LowMemoryNotification.Instance.LowMemoryState)
            {
                // we don't want to pool large buffers / clear them up on low memory
                PlatformSpecific.NativeMemory.Free4KbAlignedMemory(ptr, size, allocatingThread);
                return;
            }

            // updating the thread allocations since we released the memory back to the pool
            NativeMemory.UpdateMemoryStatsForThread(allocatingThread, size);

            var index = Bits.MostSignificantBit(size);
            _items[index].Push(new NativeAllocation
            {
                Ptr = (IntPtr)ptr,
                Size = size
            });
        }

        public void ReleaseUnmanagedResources()
        {
            foreach (var stack in _items)
            {
                while (stack.TryPop(out var allocation))
                {
                    PlatformSpecific.NativeMemory.Free4KbAlignedMemory((byte*)allocation.Ptr, allocation.Size, null);
                }
            }
        }

        public void LowMemory(LowMemorySeverity lowMemorySeverity)
        {
            if (lowMemorySeverity != LowMemorySeverity.ExtremelyLow)
                return;

            ReleaseUnmanagedResources();
        }

        public void LowMemoryOver()
        {
        }

        public EncryptionBufferStats GetStats()
        {
            var stats = new EncryptionBufferStats();

            foreach (var nativeAllocations in _items)
            {
                var totalStackSize = 0L;
                var numberOfItems = 0;

                foreach (var allocation in nativeAllocations)
                {
                    totalStackSize += allocation.Size;
                    numberOfItems++;
                }

                if (numberOfItems == 0)
                    continue;

                stats.TotalSize += totalStackSize;
                stats.TotalNumberOfItems += numberOfItems;

                stats.Details.Add(new EncryptionBufferStats.AllocationInfo
                {
                    TotalSize = totalStackSize,
                    NumberOfItems = numberOfItems,
                    AllocationSize = totalStackSize / numberOfItems
                });
            }

            return stats;
        }
    }

    public class EncryptionBufferStats : IDynamicJson
    {
        public EncryptionBufferStats()
        {
            Details = new List<AllocationInfo>();
        }

        public List<AllocationInfo> Details { get; private set; }

        public long TotalSize { get; set; }

        public Size TotalSizeHumane => new Size(TotalSize, SizeUnit.Bytes);

        public long TotalNumberOfItems { get; set; }

        public class AllocationInfo : IDynamicJson
        {
            public long TotalSize { get; set; }

            public Size TotalSizeHumane => new Size(TotalSize, SizeUnit.Bytes);

            public int NumberOfItems { get; set; }

            public long AllocationSize { get; set; }

            public Size AllocationSizeHumane => new Size(AllocationSize, SizeUnit.Bytes);

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(NumberOfItems)] = NumberOfItems,
                    [nameof(TotalSize)] = TotalSize,
                    [nameof(TotalSizeHumane)] = TotalSizeHumane.ToString(),
                    [nameof(AllocationSize)] = AllocationSize,
                    [nameof(AllocationSizeHumane)] = AllocationSizeHumane.ToString()
                };
            }
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(TotalSize)] = TotalSize,
                [nameof(TotalSizeHumane)] = TotalSizeHumane.ToString(),
                [nameof(TotalNumberOfItems)] = TotalNumberOfItems,
                [nameof(Details)] = Details.OrderByDescending(x => x.TotalSize).Select(x => x.ToJson())
            };
        }
    }
}
