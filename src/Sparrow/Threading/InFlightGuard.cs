using System;
using System.Diagnostics;
using System.Threading;
using Sparrow.Binary;
using Sparrow.Utils;

namespace Sparrow.Threading
{
    public interface ISlotCount
    {
        int Count { get; }
    }

    /// <summary>
    /// A single slot, for an instance that few threads use at once.
    /// </summary>
    public struct SingleSlot : ISlotCount
    {
        public int Count => 1;
    }

    /// <summary>
    /// A slot per core, for a shared resource that all the cores enter.
    /// </summary>
    public struct PerCoreSlots : ISlotCount
    {
        // bounds the memory on large machines, a few cores sharing a slot still contend far less than all of them
        private static readonly int Slots = Math.Min(Bits.PowerOf2(Environment.ProcessorCount), 32);

        public int Count => Slots;
    }

    /// <summary>
    /// Tracks the operations in flight on a resource: entering fails once the resource is closed, and the one who
    /// closes it can wait for the operations in flight to leave.
    /// </summary>
    public sealed class InFlightGuard<TSlots> where TSlots : struct, ISlotCount
    {
        // a slot counts its operations in flight by 2, and bit 0 marks the guard closed, so the count never reaches the flag
        private const long Closed = 1;
        private const long Operation = 2;

        // PERF: the JIT compiles the guard for each TSlots, so the slot count is a constant and a single slot has no index to compute
        private readonly PaddedLong[] _slots = new PaddedLong[default(TSlots).Count];

        // created by the one who waits, only when operations are still in flight
        private ManualResetEventSlim _drained;

        public bool IsClosed => (Volatile.Read(ref _slots[0].Value) & Closed) != 0;

        public bool TryEnter(out Scope scope)
        {
            int index = CurrentIndex();
            if ((Interlocked.Add(ref _slots[index].Value, Operation) & Closed) != 0)
            {
                Exit(index);
                scope = default;
                return false;
            }

            scope = new Scope(this, index);
            return true;
        }

        public Scope Enter(string name)
        {
            if (TryEnter(out var scope) == false)
                ThrowClosed(name);

            return scope;
        }

        /// <summary>
        /// Rejects the operations that start from now on.
        /// </summary>
        /// <returns>false when it was already closed</returns>
        public bool TryClose()
        {
            // a single caller closes, the one that sets bit 0 of the first slot
            while (true)
            {
                var value = Volatile.Read(ref _slots[0].Value);
                if ((value & Closed) != 0)
                    return false;

                if (Interlocked.CompareExchange(ref _slots[0].Value, value | Closed, value) == value)
                    break;
            }

            // the other slots are even until we close them, adding 1 sets their bit 0
            for (int i = 1; i < _slots.Length; i++)
                Interlocked.Increment(ref _slots[i].Value);

            return true;
        }

        /// <summary>
        /// Waits for the operations in flight to leave, after <see cref="TryClose"/>.
        /// </summary>
        /// <returns>false when some are still in flight after the timeout</returns>
        public bool WaitForDrain(TimeSpan timeout)
        {
            if (IsDrained())
                return true;

            var drained = Volatile.Read(ref _drained);
            if (drained == null)
            {
                drained = new ManualResetEventSlim();
                drained = Interlocked.CompareExchange(ref _drained, drained, null) ?? drained;
            }

            var sw = timeout == Timeout.InfiniteTimeSpan ? null : Stopwatch.StartNew();
            while (true)
            {
                // reset before checking, so a signal after the check is never lost
                drained.Reset();
                if (IsDrained())
                    return true;

                var wait = Timeout.Infinite;
                if (sw != null)
                {
                    var remaining = timeout - sw.Elapsed;
                    if (remaining <= TimeSpan.Zero)
                        return false;

                    wait = (int)Math.Min(remaining.TotalMilliseconds + 1, int.MaxValue);
                }

                drained.Wait(wait);
            }
        }

        private bool IsDrained()
        {
            for (int i = 0; i < _slots.Length; i++)
            {
                if ((Volatile.Read(ref _slots[i].Value) & ~Closed) != 0)
                    return false;
            }

            return true;
        }

        private void Exit(int index)
        {
            // the slot is empty and closed, the one who waits checks the other slots
            if (Interlocked.Add(ref _slots[index].Value, -Operation) == Closed)
                Volatile.Read(ref _drained)?.Set();
        }

        private static int CurrentIndex()
        {
            if (default(TSlots).Count == 1)
                return 0;

#if NETSTANDARD2_0 || NETSTANDARD2_1
            return Environment.CurrentManagedThreadId & (default(TSlots).Count - 1);
#else
            return Thread.GetCurrentProcessorId() & (default(TSlots).Count - 1);
#endif
        }

        private static void ThrowClosed(string name)
        {
            throw new LockAlreadyDisposedException(name);
        }

        public readonly struct Scope : IDisposable
        {
            private readonly InFlightGuard<TSlots> _guard;
            private readonly int _index;

            internal Scope(InFlightGuard<TSlots> guard, int index)
            {
                _guard = guard;
                _index = index;
            }

            public void Dispose()
            {
                _guard?.Exit(_index);
            }
        }
    }
}
