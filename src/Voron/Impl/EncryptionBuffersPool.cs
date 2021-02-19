using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Server;
using Sparrow.Server.Platform;
using Sparrow.Threading;
using Sparrow.Utils;
using Voron.Global;

namespace Voron.Impl
{
    public unsafe class EncryptionBuffersPool : ILowMemoryHandler
    {
        private readonly object _locker = new object();

        public static EncryptionBuffersPool Instance = new EncryptionBuffersPool();
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<EncryptionBuffersPool>("Memory");
        private const int MaxNumberOfPagesToCache = 128; // 128 * 8K = 1 MB, beyond that, we'll not both
        private readonly MultipleUseFlag _isLowMemory = new MultipleUseFlag();
        private readonly MultipleUseFlag _isExtremelyLowMemory = new MultipleUseFlag();
        private readonly PerCoreContainer<NativeAllocation>[] _items;
        private readonly CountingConcurrentStack<NativeAllocation>[] _globalStacks;
        private readonly Timer _cleanupTimer;
        private long _generation;
        public bool Disabled;
        private long _currentlyInUseBytes;

        public long Generation => _generation;

        private readonly int _maxNumberOfAllocationsToKeepInGlobalStackPerSlot;
        private long[] _numberOfAllocationsDisposedInGlobalStacks;

        private DateTime[] _lastPerCoreCleanups;
        private readonly TimeSpan _perCoreCleanupInterval = TimeSpan.FromMinutes(5);

        private DateTime[] _lastGlobalStackRebuilds;
        private readonly TimeSpan _globalStackRebuildInterval = TimeSpan.FromMinutes(15);

        public EncryptionBuffersPool()
        {
            _maxNumberOfAllocationsToKeepInGlobalStackPerSlot = PlatformDetails.Is32Bits == false
                ? 128
                : 32;

            var numberOfSlots = Bits.MostSignificantBit(MaxNumberOfPagesToCache * Constants.Storage.PageSize) + 1;
            _items = new PerCoreContainer<NativeAllocation>[numberOfSlots];
            _globalStacks = new CountingConcurrentStack<NativeAllocation>[numberOfSlots];
            _lastPerCoreCleanups = new DateTime[numberOfSlots];
            _lastGlobalStackRebuilds = new DateTime[numberOfSlots];
            _numberOfAllocationsDisposedInGlobalStacks = new long[numberOfSlots];

            var now = DateTime.UtcNow;

            for (int i = 0; i < _items.Length; i++)
            {
                _items[i] = new PerCoreContainer<NativeAllocation>();
                _globalStacks[i] = new CountingConcurrentStack<NativeAllocation>();
                _lastPerCoreCleanups[i] = now;
                _lastGlobalStackRebuilds[i] = now;
            }

            LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);

            _cleanupTimer = new Timer(Cleanup, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
        }

        public byte* Get(int numberOfPages, out long size, out NativeMemory.ThreadStats thread)
        {
            var numberOfPagesPowerOfTwo = Bits.PowerOf2(numberOfPages);

            size = numberOfPagesPowerOfTwo * Constants.Storage.PageSize;

            if (Disabled || numberOfPagesPowerOfTwo > MaxNumberOfPagesToCache)
            {
                // We don't want to pool large buffers
                size = numberOfPages * Constants.Storage.PageSize;
                Interlocked.Add(ref _currentlyInUseBytes, size);

                return PlatformSpecific.NativeMemory.Allocate4KbAlignedMemory(size, out thread);
            }

            Interlocked.Add(ref _currentlyInUseBytes, size);

            var index = Bits.MostSignificantBit(size);
            NativeAllocation allocation;
            while (_items[index].TryPull(out allocation))
            {
                if (allocation.InUse.Raise() == false)
                    continue;

                thread = NativeMemory.ThreadAllocations.Value;
                thread.Allocations += size;

                Debug.Assert(size == allocation.Size, $"size ({size}) == allocation.Size ({allocation.Size})");

                return allocation.Ptr;
            }

            var currentGlobalStack = _globalStacks[index];

            while (currentGlobalStack.TryPop(out allocation))
            {
                if (allocation.InUse.Raise() == false)
                    continue;

                Debug.Assert(size == allocation.Size, $"size ({size}) == allocation.Size ({allocation.Size})");

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

            Interlocked.Add(ref _currentlyInUseBytes, -size);

            Sodium.sodium_memzero(ptr, (UIntPtr)size);

            var numberOfPages = size / Constants.Storage.PageSize;

            if (Disabled || numberOfPages > MaxNumberOfPagesToCache || (_isLowMemory.IsRaised() && generation < Generation))
            {
                // - don't want to pool large buffers
                // - release all the buffers that were created before we got the low memory event
                PlatformSpecific.NativeMemory.Free4KbAlignedMemory(ptr, size, allocatingThread);
                return;
            }

            var index = Bits.MostSignificantBit(size);
            var allocation = new NativeAllocation
            {
                Ptr = ptr,
                Size = size,
                InPoolSince = DateTime.UtcNow
            };

            var addToPerCorePool = ForTestingPurposes == null || ForTestingPurposes.CanAddToPerCorePool;
            var success = addToPerCorePool ? _items[index].TryPush(allocation) : false;

            if (success)
            {
                // updating the thread allocations since we released the memory back to the pool
                NativeMemory.UpdateMemoryStatsForThread(allocatingThread, size);
                return;
            }

            var addToGlobalPool = ForTestingPurposes == null || ForTestingPurposes.CanAddToGlobalPool;

            var currentGlobalStack = _globalStacks[index];
            if (addToGlobalPool && currentGlobalStack.Count < _maxNumberOfAllocationsToKeepInGlobalStackPerSlot)
            {
                // updating the thread allocations since we released the memory back to the pool
                NativeMemory.UpdateMemoryStatsForThread(allocatingThread, size);
                currentGlobalStack.Push(allocation);
                return;
            }

            PlatformSpecific.NativeMemory.Free4KbAlignedMemory(ptr, size, allocatingThread);
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

            for (int i = 0; i < _items.Length; i++)
            {
                ClearStack(_globalStacks[i]);

                foreach (var allocation in _items[i].EnumerateAndClear())
                {
                    if (allocation.InUse.Raise())
                        allocation.Dispose();
                }
            }

            static void ClearStack(CountingConcurrentStack<NativeAllocation> stack)
            {
                if (stack == null || stack.IsEmpty)
                    return;

                while (stack.TryPop(out var allocation))
                {
                    if (allocation.InUse.Raise())
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
            stats.Disabled = Disabled;
            stats.CurrentlyInUseSize = _currentlyInUseBytes;

            for (int i = 0; i < _items.Length; i++)
            {
                var totalStackSize = 0L;
                var totalGlobalStackSize = 0L;

                var numberOfItems = 0;
                var numberOfGlobalStackItems = 0;

                foreach (var (allocation, _) in _items[i])
                {
                    if (allocation.InUse.IsRaised())
                    {
                        // not in the pool or disposed
                        continue;
                    }

                    totalStackSize += allocation.Size;
                    numberOfItems++;
                }

                foreach (var allocation in _globalStacks[i])
                {
                    if (allocation.InUse.IsRaised())
                    {
                        // not in the pool or disposed
                        continue;
                    }

                    totalGlobalStackSize += allocation.Size;
                    numberOfGlobalStackItems++;
                }

                if (numberOfItems > 0)
                {
                    stats.TotalPoolSize += totalStackSize;
                    stats.TotalNumberOfItems += numberOfItems;

                    stats.Details.Add(new EncryptionBufferStats.AllocationInfo
                    {
                        AllocationType = EncryptionBufferStats.AllocationType.PerCore,
                        TotalSize = totalStackSize,
                        NumberOfItems = numberOfItems,
                        AllocationSize = totalStackSize / numberOfItems
                    });
                }

                if (numberOfGlobalStackItems > 0)
                {
                    stats.TotalPoolSize += totalGlobalStackSize;
                    stats.TotalNumberOfItems += numberOfGlobalStackItems;

                    stats.Details.Add(new EncryptionBufferStats.AllocationInfo
                    {
                        AllocationType = EncryptionBufferStats.AllocationType.Global,
                        TotalSize = totalGlobalStackSize,
                        NumberOfItems = numberOfGlobalStackItems,
                        AllocationSize = totalGlobalStackSize / numberOfGlobalStackItems
                    });
                }
            }

            return stats;
        }

        private void Cleanup(object _)
        {
            if (Monitor.TryEnter(_locker) == false)
                return;

            try
            {
                var currentTime = DateTime.UtcNow;
                var idleTime = TimeSpan.FromMinutes(10);

                for (int i = 0; i < _items.Length; i++)
                {
                    var currentStack = _items[i];
                    var currentGlobalStack = _globalStacks[i];

                    var perCoreCleanupNeeded = currentGlobalStack.IsEmpty || currentTime - _lastPerCoreCleanups[i] >= _perCoreCleanupInterval;
                    if (perCoreCleanupNeeded)
                    {
                        _lastPerCoreCleanups[i] = currentTime;

                        foreach (var current in currentStack)
                        {
                            var allocation = current.Item;
                            var timeInPool = currentTime - allocation.InPoolSince;
                            if (timeInPool <= idleTime)
                                continue;

                            if (allocation.InUse.Raise() == false)
                                continue;

                            currentStack.Remove(current.Item, current.Pos);
                            allocation.Dispose();
                        }

                        continue;
                    }

                    using (var globalStackEnumerator = currentGlobalStack.GetEnumerator())
                    {
                        while (globalStackEnumerator.MoveNext())
                        {
                            var allocation = globalStackEnumerator.Current;

                            var timeInPool = currentTime - allocation.InPoolSince;
                            if (timeInPool <= idleTime)
                                continue;

                            if (allocation.InUse.Raise() == false)
                                continue;

                            allocation.Dispose();
                            _numberOfAllocationsDisposedInGlobalStacks[i]++;
                        }
                    }

                    var globalStackRebuildNeeded = currentTime - _lastGlobalStackRebuilds[i] >= _globalStackRebuildInterval;

                    if (globalStackRebuildNeeded && _numberOfAllocationsDisposedInGlobalStacks[i] > 0)
                    {
                        _lastGlobalStackRebuilds[i] = currentTime;

                        _numberOfAllocationsDisposedInGlobalStacks[i] = 0;

                        var localStack = new CountingConcurrentStack<NativeAllocation>();

                        while (currentGlobalStack.TryPop(out var allocation))
                        {
                            if (allocation.InUse.Raise() == false)
                                continue;

                            allocation.InUse.Lower();
                            localStack.Push(allocation);
                        }

                        while (localStack.TryPop(out var allocation))
                            currentGlobalStack.Push(allocation);
                    }
                }
            }
            catch (Exception e)
            {
                Debug.Assert(e is OutOfMemoryException, $"Expecting OutOfMemoryException but got: {e}");
                if (Logger.IsOperationsEnabled)
                    Logger.Operations("Error during cleanup.", e);
            }
            finally
            {
                Monitor.Exit(_locker);
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

        internal TestingStuff ForTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (ForTestingPurposes != null)
                return ForTestingPurposes;

            return ForTestingPurposes = new TestingStuff();
        }

        internal class TestingStuff
        {
            public bool CanAddToPerCorePool = true;

            public bool CanAddToGlobalPool = true;
        }
    }

    public class EncryptionBufferStats : IDynamicJson
    {
        public EncryptionBufferStats()
        {
            Details = new List<AllocationInfo>();
        }

        public bool Disabled { get; set; }

        public List<AllocationInfo> Details { get; private set; }

        public long TotalPoolSize { get; set; }

        public long CurrentlyInUseSize { get; set; }

        public Size CurrentlyInUseSizeHumane => new Size(CurrentlyInUseSize, SizeUnit.Bytes);

        public Size TotalPoolSizeHumane => new Size(TotalPoolSize, SizeUnit.Bytes);

        public long TotalNumberOfItems { get; set; }

        public class AllocationInfo : IDynamicJson
        {
            public AllocationType AllocationType { get; set; }

            public long TotalSize { get; set; }

            public Size TotalSizeHumane => new Size(TotalSize, SizeUnit.Bytes);

            public int NumberOfItems { get; set; }

            public long AllocationSize { get; set; }

            public Size AllocationSizeHumane => new Size(AllocationSize, SizeUnit.Bytes);

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(AllocationType)] = AllocationType,
                    [nameof(NumberOfItems)] = NumberOfItems,
                    [nameof(TotalSize)] = TotalSize,
                    [nameof(TotalSizeHumane)] = TotalSizeHumane.ToString(),
                    [nameof(AllocationSize)] = AllocationSize,
                    [nameof(AllocationSizeHumane)] = AllocationSizeHumane.ToString()
                };
            }
        }

        public enum AllocationType
        {
            PerCore,
            Global
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Disabled)] = Disabled,
                [nameof(CurrentlyInUseSize)] = CurrentlyInUseSize,
                [nameof(CurrentlyInUseSizeHumane)] = CurrentlyInUseSizeHumane.ToString(),
                [nameof(TotalPoolSize)] = TotalPoolSize,
                [nameof(TotalPoolSizeHumane)] = TotalPoolSizeHumane.ToString(),
                [nameof(TotalNumberOfItems)] = TotalNumberOfItems,
                [nameof(Details)] = Details.OrderByDescending(x => x.TotalSize).Select(x => x.ToJson())
            };
        }
    }
}
