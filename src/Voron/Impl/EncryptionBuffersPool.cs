using System;
using System.Collections;
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
using Sparrow.Server.Debugging;
using Sparrow.Server.Logging;
using Sparrow.Server.Platform;
using Sparrow.Threading;
using Sparrow.Utils;
using Voron.Global;
using Voron.Impl.Paging;
using Voron.Logging;

namespace Voron.Impl
{
    public sealed unsafe class EncryptionBuffersPool : ILowMemoryHandler
    {
        private readonly object _locker = new object();

        public static EncryptionBuffersPool Instance = new EncryptionBuffersPool();
        private static readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForGlobalVoron<EncryptionBuffersPool>();
        private const int MaxNumberOfPagesToCache = 128; // 128 * 8K = 1 MB, beyond that, we'll not both
        private readonly MultipleUseFlag _isLowMemory = new MultipleUseFlag();
        private readonly MultipleUseFlag _isExtremelyLowMemory = new MultipleUseFlag();
        private readonly PerCoreContainer<NativeAllocation, NativeAllocationContainerPolicy>[] _items;
        private readonly SlotState[] _slotStates;
        private readonly Timer _cleanupTimer;
        private long _generation;
        public bool Disabled;
        private long _currentlyInUseBytes;
        private readonly TimeSpan _idleTime = TimeSpan.FromMinutes(10);
        private DateTime _lastCleanupTime = DateTime.MinValue;
        private readonly TimeSpan _cleanupInterval = TimeSpan.FromMinutes(2);

        public long Generation => _generation;

        private readonly int _maxNumberOfAllocationsToKeepInGlobalStackPerSlot;

        public EncryptionBuffersPool(bool registerLowMemory = true, bool registerCleanup = true)
        {
            _maxNumberOfAllocationsToKeepInGlobalStackPerSlot = PlatformDetails.Is32Bits == false ? 128 : 32;

            var numberOfSlots = Bits.MostSignificantBit(MaxNumberOfPagesToCache * Constants.Storage.PageSize) + 1;
            _items = new PerCoreContainer<NativeAllocation, NativeAllocationContainerPolicy>[numberOfSlots];
            _slotStates = new SlotState[numberOfSlots];

            for (int i = 0; i < _items.Length; i++)
            {
                ref var state = ref _slotStates[i];
                state = new SlotState { Size = 1L << i };
                state.Policy = new NativeAllocationContainerPolicy(_slotStates[i], _idleTime, () => DateTime.UtcNow);

                // Increase shared ring buffer capacity to compensate for removed global stacks
                var globalSlots = _maxNumberOfAllocationsToKeepInGlobalStackPerSlot * 2;
                _items[i] = new PerCoreContainer<NativeAllocation, NativeAllocationContainerPolicy>(
                    numberOfGlobalSlots: globalSlots,
                    numberOfSlotsPerCore: 64,
                    policy: state.Policy);
            }

            if (registerLowMemory)
                LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);

            if (registerCleanup)
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
                ForTestingPurposes?.OnFree4KbAlignedMemory?.Invoke(size);
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
            var success = addToPerCorePool && _items[index].TryPush(allocation);

            if (success)
            {
                // updating the thread allocations since we released the memory back to the pool
                ForTestingPurposes?.OnUpdateMemoryStatsForThread?.Invoke(size);
                NativeMemory.UpdateMemoryStatsForThread(allocatingThread, size);
                return;
            }

            ForTestingPurposes?.OnFree4KbAlignedMemory?.Invoke(size);
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

            // Use RemoveAllPolicy to clear everything regardless of idle time
            var removeAllPolicy = new RemoveAllContainerPolicy(_slotStates);
            for (int i = 0; i < _items.Length; i++)
            {
                _items[i].Cleanup(removeAllPolicy, allocation =>
                {
                    if (allocation.InUse.Raise())
                        allocation.Dispose();
                });
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
                var slotState = _slotStates[i];

                var numberOfItems = Volatile.Read(ref slotState.Count);
                var totalBytes = Volatile.Read(ref slotState.Bytes);

                if (numberOfItems > 0)
                {
                    stats.TotalPoolSize += totalBytes;
                    stats.TotalNumberOfItems += numberOfItems;

                    stats.Details.Add(new EncryptionBufferStats.AllocationInfo
                    {
                        AllocationType = EncryptionBufferStats.AllocationType.Unified,
                        TotalSize = totalBytes,
                        NumberOfItems = (int)numberOfItems,
                        AllocationSize = totalBytes / numberOfItems
                    });
                }
            }

            return stats;
        }

        private void Cleanup(object _)
        {
            var currentTime = DateTime.UtcNow;
            if (currentTime - _lastCleanupTime < _cleanupInterval)
                return;

            if (Monitor.TryEnter(_locker) == false)
                return;

            try
            {
                _lastCleanupTime = currentTime;

                for (int i = 0; i < _items.Length; i++)
                {
                    // Use default policy for time-based cleanup
                    _items[i].Cleanup(_slotStates[i].Policy, allocation =>
                    {
                        if (allocation.InUse.Raise())
                            allocation.Dispose();
                    });
                }
            }
            catch (Exception e)
            {
                Debug.Assert(e is OutOfMemoryException, $"Expecting OutOfMemoryException but got: {e}");
                if (Logger.IsErrorEnabled)
                    Logger.Error("Error during cleanup.", e);
            }
            finally
            {
                Monitor.Exit(_locker);
            }
        }

        private sealed class NativeAllocation : PooledItem
        {
            public byte* Ptr;
            public long Size;

            public override void Dispose()
            {
                PlatformSpecific.NativeMemory.Free4KbAlignedMemory(Ptr, Size, null);
            }
        }

        private sealed class SlotState
        {
            public long Size;
            public long Count;
            public long Bytes;
            public NativeAllocationContainerPolicy Policy;
        }

        private readonly struct NativeAllocationContainerPolicy : IPerCoreContainerPolicy<NativeAllocation>
        {
            private readonly SlotState _state;
            private readonly TimeSpan _idleTime;
            private readonly Func<DateTime> _currentTimeProvider;

            public NativeAllocationContainerPolicy(SlotState state, TimeSpan idleTime, Func<DateTime> currentTimeProvider)
            {
                _state = state;
                _idleTime = idleTime;
                _currentTimeProvider = currentTimeProvider;
            }

            public bool CanRemove => true;

            public bool ShouldRemove(NativeAllocation item, int coreIndex)
            {
                var timeInPool = _currentTimeProvider() - item.InPoolSince;
                return timeInPool > _idleTime;
            }

            public void OnAdded(NativeAllocation item, int coreIndex)
            {
                Interlocked.Increment(ref _state.Count);
                Interlocked.Add(ref _state.Bytes, _state.Size);
            }

            public void OnRemoved(NativeAllocation item, int coreIndex)
            {
                Interlocked.Decrement(ref _state.Count);
                Interlocked.Add(ref _state.Bytes, -_state.Size);
            }
        }

        private readonly struct RemoveAllContainerPolicy : IPerCoreContainerPolicy<NativeAllocation>
        {
            private readonly SlotState[] _slotStates;

            public RemoveAllContainerPolicy(SlotState[] slotStates)
            {
                _slotStates = slotStates;
            }

            public bool CanRemove => true;

            public bool ShouldRemove(NativeAllocation item, int coreIndex) => true;

            public void OnAdded(NativeAllocation item, int coreIndex) => throw new NotSupportedException();

            public void OnRemoved(NativeAllocation item, int coreIndex)
            {
                // Find the slot index from the allocation size
                var index = Bits.MostSignificantBit(item.Size);
                var slotState = _slotStates[index];
                Interlocked.Decrement(ref slotState.Count);
                Interlocked.Add(ref slotState.Bytes, -slotState.Size);
            }
        }

        internal TestingStuff ForTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (ForTestingPurposes != null)
                return ForTestingPurposes;

            return ForTestingPurposes = new TestingStuff();
        }

        internal sealed class TestingStuff
        {
            public bool CanAddToPerCorePool = true;

            public Action<long> OnFree4KbAlignedMemory;

            public Action<long> OnUpdateMemoryStatsForThread;
        }
    }

    public sealed class EncryptionBufferStats : IDynamicJson
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

        public sealed class AllocationInfo : IDynamicJson
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
            Global,
            Unified
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
