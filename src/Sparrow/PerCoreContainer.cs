using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow.Collections;
using Sparrow.LowMemory;
using Sparrow.Utils;

namespace Sparrow.Json
{
    public interface IPerCoreContainerPolicy<T>
        where T : class
    {
        bool CanRemove { get; }
        bool ShouldRemove(T item, int coreIndex);
        void OnAdded(T item, int coreIndex);
        void OnRemoved(T item, int coreIndex);
    }

    public struct NoOpContainerPolicy<T> : IPerCoreContainerPolicy<T>
        where T : class
    {
        public bool CanRemove => false;
        public bool ShouldRemove(T item, int coreIndex) => false;
        public void OnAdded(T item, int coreIndex) { }
        public void OnRemoved(T item, int coreIndex) { }
    }

    public struct LowMemoryRemovalPolicy<T> : IPerCoreContainerPolicy<T>
        where T : class
    {
        public bool CanRemove => LowMemoryNotification.Instance != null && LowMemoryNotification.Instance.LowMemoryState;

        public bool ShouldRemove(T item, int coreIndex) => CanRemove;

        public void OnAdded(T item, int coreIndex)
        {
            // no-op
        }

        public void OnRemoved(T item, int coreIndex)
        {
            if (item is IDisposable d)
                d.Dispose();
        }
    }

    public struct RemoveAllPolicy<T> : IPerCoreContainerPolicy<T>
        where T : class
    {
        public bool CanRemove => true;

        public bool ShouldRemove(T item, int coreIndex) => true;

        public void OnAdded(T item, int coreIndex)
        {
            // no-op
        }

        public void OnRemoved(T item, int coreIndex)
        {
            // no-op - caller handles disposal
        }
    }

    public struct RemoveAllAndDisposePolicy<T> : IPerCoreContainerPolicy<T>
        where T : class, IDisposable
    {
        public bool CanRemove => true;

        public bool ShouldRemove(T item, int coreIndex) => true;

        public void OnAdded(T item, int coreIndex)
        {
            // no-op
        }

        public void OnRemoved(T item, int coreIndex)
        {
            item?.Dispose();
        }
    }

    public sealed class PerCoreContainer<T> : PerCoreContainer<T, NoOpContainerPolicy<T>>
        where T : class
    {
        public PerCoreContainer(int numberOfGlobalSlots = 1024, int numberOfSlotsPerCore = 64)
            : base(numberOfGlobalSlots, numberOfSlotsPerCore)
        {
        }
    }

    public class PerCoreContainer<T, TPolicy>
        where T : class
        where TPolicy : struct, IPerCoreContainerPolicy<T>
    {
        [StructLayout(LayoutKind.Sequential, Size = 128)]
        private struct PerCoreSlotContainer
        {
            public int Value;
            public T[] Container;
        }

        private readonly int _numberOfSlotsPerCore;
        private readonly PerCoreSlotContainer[] _perCoreArrays;
        private readonly LockFreeRingBuffer<T> _sharedContainer;
        private readonly TPolicy _defaultPolicy;

        public PerCoreContainer(int numberOfGlobalSlots = 1024, int numberOfSlotsPerCore = 64, TPolicy policy = default)
        {
            _numberOfSlotsPerCore = numberOfSlotsPerCore;
            _defaultPolicy = policy;

            var coreCount = Environment.ProcessorCount;
            var numberOfSharedSlots = Math.Max(numberOfGlobalSlots, 2 * numberOfSlotsPerCore * coreCount);
            _sharedContainer = new LockFreeRingBuffer<T>(numberOfSharedSlots);

            _perCoreArrays = new PerCoreSlotContainer[coreCount];
            for (var i = 0; i < coreCount; i++)
            {
                _perCoreArrays[i].Container = new T[numberOfSlotsPerCore];
            }
        }

        public bool TryPull(out T output)
        {
            var currentProcessorId = CurrentProcessorIdHelper.GetCurrentProcessorId() % _perCoreArrays.Length;
            ref var perCore = ref _perCoreArrays[currentProcessorId];

            if (Volatile.Read(ref perCore.Value) > 0)
            {
                var coreItems = perCore.Container;

                for (var i = 0; i < coreItems.Length; i++)
                {
                    var cur = coreItems[i];
                    if (cur == null)
                        continue;

                    if (Interlocked.CompareExchange(ref coreItems[i], null, cur) != cur)
                        continue;

                    Interlocked.Decrement(ref perCore.Value);
                    _defaultPolicy.OnRemoved(cur, currentProcessorId);
                    output = cur;
                    return true;
                }
            }

            if (_sharedContainer.TryDequeue(out output))
            {
                _defaultPolicy.OnRemoved(output, -1);
                return true;
            }

            output = default;
            return false;
        }

        public bool TryPush(T cur)
        {
            var currentProcessorId = CurrentProcessorIdHelper.GetCurrentProcessorId() % _perCoreArrays.Length;
            ref var perCore = ref _perCoreArrays[currentProcessorId];

            if (Volatile.Read(ref perCore.Value) < _numberOfSlotsPerCore)
            {
                var core = perCore.Container;

                for (var i = 0; i < core.Length; i++)
                {
                    if (core[i] != null)
                        continue;

                    if (Interlocked.CompareExchange(ref core[i], cur, null) == null)
                    {
                        Interlocked.Increment(ref perCore.Value);
                        _defaultPolicy.OnAdded(cur, currentProcessorId);
                        return true;
                    }
                }
            }

            if (_sharedContainer.TryEnqueue(cur))
            {
                _defaultPolicy.OnAdded(cur, -1);
                return true;
            }

            return false;
        }

        public void Cleanup<TCleanupPolicy>(TCleanupPolicy policy)
            where TCleanupPolicy : struct, IPerCoreContainerPolicy<T>
        {
            if (policy.CanRemove == false)
                return;

            // Clean per-core arrays
            for (var gi = 0; gi < _perCoreArrays.Length; gi++)
            {
                ref var perCore = ref _perCoreArrays[gi];

                var array = perCore.Container;

                // Find and atomically remove an item
                for (var li = 0; li < array.Length; li++)
                {
                    var current = array[li];
                    if (current == null)
                        continue;

                    if (Interlocked.CompareExchange(ref array[li], null, current) != current)
                        continue;

                    Interlocked.Decrement(ref perCore.Value);

                    // Now decide what to do with the atomically removed item
                    if (policy.ShouldRemove(current, gi))
                    {
                        policy.OnRemoved(current, gi);
                        continue;
                    }

                    // Try to put it back in the shared container, if it fails, we have to remove it.
                    if (_sharedContainer.TryEnqueue(current) == false)
                    {
                        policy.OnRemoved(current, gi);
                    }
                }
            }

            if (_sharedContainer.IsEmpty)
                return;

            // Clean shared container
            int count = _sharedContainer.Count;
            for (int i = 0; i < count; i++)
            {
                if (_sharedContainer.TryDequeue(out var item) == false)
                    break;

                if (policy.ShouldRemove(item, -1))
                {
                    policy.OnRemoved(item, -1);
                    continue;
                }

                // Try to put it back in the shared container, if it fails, we have to remove it.
                if (_sharedContainer.TryEnqueue(item) == false)
                {
                    policy.OnRemoved(item, -1);
                }
            }
        }     

        public void Cleanup<TCleanupPolicy>(TCleanupPolicy policy, Action<T> action)
            where TCleanupPolicy : struct, IPerCoreContainerPolicy<T>
        {
            if (policy.CanRemove == false)
                return;

            // Clean per-core arrays
            for (var gi = 0; gi < _perCoreArrays.Length; gi++)
            {
                ref var perCore = ref _perCoreArrays[gi];
                
                var array = perCore.Container;

                // Find and atomically remove an item
                for (var li = 0; li < array.Length; li++)
                {
                    var current = array[li];
                    if (current == null)
                        continue;

                    if (Interlocked.CompareExchange(ref array[li], null, current) != current)
                        continue;

                    Interlocked.Decrement(ref perCore.Value);

                    // Now decide what to do with the atomically removed item
                    if (policy.ShouldRemove(current, gi))
                    {
                        policy.OnRemoved(current, gi);
                        action(current);
                        continue;
                    }

                    // Try to put it back in the shared container, if it fails, we have to remove it.
                    if (_sharedContainer.TryEnqueue(current) == false)
                    {
                        policy.OnRemoved(current, gi);
                        action(current);
                    }
                }
            }

            if (_sharedContainer.IsEmpty)
                return;

            // Clean shared container
            int count = _sharedContainer.Count;
            for ( int i = 0; i < count; i++)
            {
                if (_sharedContainer.TryDequeue(out var item) == false)
                    break;

                if (policy.ShouldRemove(item, -1))
                {
                    policy.OnRemoved(item, -1);
                    action(item);
                    continue;
                }

                // Try to put it back in the shared container, if it fails, we have to remove it.
                if (_sharedContainer.TryEnqueue(item) == false)
                {
                    policy.OnRemoved(item, -1);
                    action(item);
                }
            }
        }
    }
}
