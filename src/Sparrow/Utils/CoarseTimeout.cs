using System;
using System.Threading;

namespace Sparrow.Utils
{
    /// <summary>
    /// Timeouts for operations that rarely time out. A single timer moves a hand over a ring of slots and cancels the
    /// shared token of each slot it reaches, so a long timeout is two volatile reads: no allocation, no timer, no lock.
    /// A timeout too short (or too long) for the ring gets a token source of its own, which <see cref="TimeoutToken.Dispose"/> releases.
    /// </summary>
    internal static class CoarseTimeout
    {
        private const int GranularityInMs = 15_000;

        // the shared tokens fire up to two slots late, too imprecise for shorter timeouts
        private const int MinimumSharedTimeoutInMs = 4 * GranularityInMs;

        private const int SlotCount = 64;

        private static readonly CancellationTokenSource[] Slots = CreateSlots();
        private static long _hand;
        private static readonly Timer Ticker = new(OnTick, null, GranularityInMs, GranularityInMs);

        public static TimeoutToken After(int timeoutInMs)
        {
            if (timeoutInMs == Timeout.Infinite)
                return default;

            // one extra slot, so we never take the slot that the hand is about to reach
            long slotsAhead = (timeoutInMs + GranularityInMs - 1) / GranularityInMs + 1;
            if (timeoutInMs < MinimumSharedTimeoutInMs || slotsAhead >= SlotCount)
                return new TimeoutToken(new CancellationTokenSource(timeoutInMs));

            var hand = Volatile.Read(ref _hand);
            return new TimeoutToken(Volatile.Read(ref Slots[(int)((hand + slotsAhead) % SlotCount)]).Token);
        }

        public readonly struct TimeoutToken : IDisposable
        {
            private readonly CancellationTokenSource _owned;

            public readonly CancellationToken Token;

            internal TimeoutToken(CancellationToken shared)
            {
                Token = shared;
            }

            internal TimeoutToken(CancellationTokenSource owned)
            {
                _owned = owned;
                Token = owned.Token;
            }

            /// <summary>
            /// The token belongs to a shared slot, nothing to release.
            /// </summary>
            public bool IsShared => _owned == null;

            public void Dispose()
            {
                _owned?.Dispose();
            }
        }

        private static CancellationTokenSource[] CreateSlots()
        {
            var slots = new CancellationTokenSource[SlotCount];
            for (int i = 0; i < slots.Length; i++)
                slots[i] = new CancellationTokenSource();
            return slots;
        }

        private static void OnTick(object state)
        {
            // a slow tick (many callbacks to run) must not overlap with the next one
            if (Monitor.TryEnter(Slots) == false)
                return;

            try
            {
                var hand = Interlocked.Increment(ref _hand);
                var expired = Interlocked.Exchange(ref Slots[(int)(hand % SlotCount)], new CancellationTokenSource());

                // not disposed, tokens handed out from it may still be registered to
                expired.Cancel();
            }
            catch
            {
                // a failing callback belongs to its operation, the ticker has to keep going
            }
            finally
            {
                Monitor.Exit(Slots);
            }
        }
    }
}
