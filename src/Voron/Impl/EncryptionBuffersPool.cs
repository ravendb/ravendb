using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.LowMemory;
using Sparrow.Server;
using Sparrow.Server.Platform;
using Sparrow.Threading;
using Sparrow.Utils;
using Voron.Global;

namespace Voron.Impl
{
    public unsafe class EncryptionBuffersPool : ILowMemoryHandler
    {
        public static EncryptionBuffersPool Instance = new EncryptionBuffersPool();
        private const int MaxNumberOfPagesToCache = 128; // 128 * 8K = 1 MB, beyond that, we'll not both 
        private readonly MultipleUseFlag _isLowMemory = new MultipleUseFlag();
        private readonly MultipleUseFlag _isExtremelyLowMemory = new MultipleUseFlag();
        private readonly PerCoreContainer<NativeAllocation>[] _items;
        private readonly Timer _cleanupTimer;
        private DateTime _lastAllocationsRebuild = DateTime.UtcNow;
        private long _generation;
        private bool _hasDisposedAllocations;
        public bool Disabled;

        public long Generation => _generation;

        public EncryptionBuffersPool()
        {
            var numberOfSlots = Bits.MostSignificantBit(MaxNumberOfPagesToCache * Constants.Storage.PageSize) + 1;
            _items = new PerCoreContainer<NativeAllocation>[numberOfSlots];

            for (int i = 0; i < _items.Length; i++)
            {
                _items[i] = new PerCoreContainer<NativeAllocation>();
            }

            LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);

            _cleanupTimer = new Timer(CleanupTimer, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
        }

        public byte* Get(int numberOfPages, out int size, out NativeMemory.ThreadStats thread)
        {
            numberOfPages = Bits.PowerOf2(numberOfPages); ;
            size = numberOfPages * Constants.Storage.PageSize;

            if (Disabled || numberOfPages > MaxNumberOfPagesToCache)
            {
                // We don't want to pool large buffers
                return PlatformSpecific.NativeMemory.Allocate4KbAlignedMemory(size, out thread);
            }

            var index = Bits.MostSignificantBit(size);
            while (_items[index].TryPull(out var allocation))
            {
                if (allocation.InUse.Raise() == false)
                    continue;

                thread = NativeMemory.ThreadAllocations.Value;
                thread.Allocations += size;
                return allocation.Ptr;
            }

            return PlatformSpecific.NativeMemory.Allocate4KbAlignedMemory(size, out thread);
        }

        public void Return(byte* ptr, long size, NativeMemory.ThreadStats allocatingThread, long generation)
        {
            if (ptr == null)
                return;

            Sodium.sodium_memzero(ptr, (UIntPtr)size);

            if (Disabled || size / Constants.Storage.PageSize > MaxNumberOfPagesToCache || 
                (_isLowMemory.IsRaised() && generation < Generation))
            {
                // - don't want to pool large buffers
                // - release all the buffers that were created before we got the low memory event
                PlatformSpecific.NativeMemory.Free4KbAlignedMemory(ptr, size, allocatingThread);
                return;
            }

            // updating the thread allocations since we released the memory back to the pool
            NativeMemory.UpdateMemoryStatsForThread(allocatingThread, size);

            var index = Bits.MostSignificantBit(size);
            _items[index].Push(new NativeAllocation
            {
                Ptr = ptr,
                Size = size,
                InPoolSince = DateTime.UtcNow
            });
        }

        public void LowMemory(LowMemorySeverity lowMemorySeverity)
        {
            if (_isLowMemory.Raise())
            {
                Interlocked.Increment(ref _generation);
            }

            if (lowMemorySeverity != LowMemorySeverity.ExtremelyLow)
                return;

            if (_isExtremelyLowMemory.Raise() == false)
                return;

            foreach (var container in _items)
            {
                while (container.TryPull(out var allocation))
                {
                    if (allocation.InUse.Raise() == false)
                        continue;

                    allocation.Dispose();
                }
            }
        }

        public void LowMemoryOver()
        {
            _isLowMemory.Lower();
            _isExtremelyLowMemory.Lower();
        }

        public EncryptionBufferStats GetStats()
        {
            var stats = new EncryptionBufferStats();

            foreach (var nativeAllocations in _items)
            {
                var totalStackSize = 0L;
                var numberOfItems = 0;

                foreach (var (allocation,_) in nativeAllocations)
                {
                    if (allocation.InUse.IsRaised())
                    {
                        // not in the pool or disposed
                        continue;
                    }

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

        private void CleanupTimer(object _)
        {
            if (Monitor.TryEnter(this) == false)
                return;

            try
            {
                var currentTime = DateTime.UtcNow;
                var idleTime = TimeSpan.FromMinutes(10);
                var allocationsRebuildInterval = TimeSpan.FromMinutes(15);

                foreach (var container in _items)
                {
                    var currentStack = container;
                    using (var currentStackEnumerator = currentStack.GetEnumerator())
                    {
                        while (currentStackEnumerator.MoveNext())
                        {
                            var (nativeAllocation, _) = currentStackEnumerator.Current;

                            var timeInPool = currentTime - nativeAllocation.InPoolSince;
                            if (timeInPool <= idleTime)
                                continue;

                            if (nativeAllocation.InUse.Raise() == false)
                                continue;

                            nativeAllocation.Dispose();
                            _hasDisposedAllocations = true;
                        }
                    }
                }

                var allocationsRebuildNeeded = currentTime - _lastAllocationsRebuild >= allocationsRebuildInterval;
                if (allocationsRebuildNeeded && _hasDisposedAllocations)
                {
                    _lastAllocationsRebuild = currentTime;
                    _hasDisposedAllocations = false;

                    foreach (var container in _items)
                    {
                        var localStack = new Stack<NativeAllocation>();

                        while (container.TryPull(out var allocation))
                        {
                            if (allocation.InUse.Raise() == false)
                                continue;

                            allocation.InUse.Lower();
                            localStack.Push(allocation);
                        }

                        while (localStack.TryPop(out var allocation))
                            container.Push(allocation);
                    }
                }
            }
            finally
            {
                Monitor.Exit(this);
            }
        }

        private class NativeAllocation : PooledItem
        {
            public byte* Ptr;
            public long Size;

            public override void Dispose()
            {
                PlatformSpecific.NativeMemory.Free4KbAlignedMemory(Ptr, Size, null);
            }
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
