using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Sparrow.Server
{
    /// <summary>
    /// Many-reader / rare-writer lock specialized for the "drain readers
    /// before maintenance" contract. Optimized for an uncontended reader
    /// hot path: one atomic op per acquire, zero allocations.
    ///
    /// Contract:
    ///   - Readers are non-exclusive among themselves.
    ///   - A pending or active writer blocks new readers (no writer starvation).
    ///   - Writers are serialized; a writer waits until readers drain to zero.
    ///   - No reentrancy; callers track that externally if they need it.
    ///
    /// State (single 64-bit word, atomic):
    ///   bit 0          WriterPending - blocks new readers.
    ///   bits 1..62     active reader count, stored shifted (each reader is +/- 2).
    ///   bit 63 (sign)  reachable only on overflow/underflow - a bug, fail loudly.
    ///
    /// Reader fast path is unconditional Interlocked.Add followed by
    /// inspect-and-undo if WriterPending is set in the post-add state. This
    /// bounds the hot path at one atomic op even under contention; a CAS-retry
    /// loop would devolve into a cache-line storm at high reader counts.
    /// The counter can briefly bump above zero during an aborted acquire, but
    /// the aborting reader decrements before doing protected work, so the
    /// writer's exclusivity is intact. The writer's drain decision is anchored
    /// on the count snapshot Interlocked.Or observes when publishing the flag.
    ///
    /// Usage:
    ///   Stack RAII:    using var h = lock.EnterRead(token);
    ///   Holder class:  AcquireRead/TryAcquireRead + ReleaseRead, paired
    ///                  with a bool _heldLock and a Debug finalizer.
    /// Forgotten release leaks the read - next writer hangs to timeout.
    /// TODO: Roslyn analyzer to require `using` on every ReadHandle local.
    /// </summary>
    public sealed class ReaderDrainLock : IDisposable
    {
        // Encoding rationale: by stepping readers in +/- 2 and parking the
        // writer flag in bit 0, no reader arithmetic can ever flip the flag
        // (carries propagate upward, never down through bit 0). The sign bit
        // doubles as overflow/underflow detection: any reach into it is a
        // bug, caught by a single `s < 0` check on every reader op.
        private const long WriterPendingBit = 1L;
        private const long ReaderUnit = 2L;

        private long _state;

        // Set when no readers are active or no writer is pending. The writer
        // waits on this; readers signal it on the 1->0 transition while a
        // writer is pending.
        private readonly ManualResetEventSlim _drained = new ManualResetEventSlim(initialState: true, spinCount: 0);

        // Set when no writer is pending. Readers wait on this in the slow
        // path; a writer resets it on entry and sets it on exit.
        private readonly ManualResetEventSlim _writerCleared = new ManualResetEventSlim(initialState: true, spinCount: 0);

        // Serializes writers. Held from EnterWrite to WriteHandle.Dispose.
        private readonly object _writerGate = new object();

        private bool _disposed;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryAcquire()
        {
            long s = Interlocked.Add(ref _state, ReaderUnit);
            if (s < 0)
                ThrowReaderOverflow();
            if ((s & WriterPendingBit) == 0)
                return true;

            // Writer beat us. Undo and, if our undo emptied the count
            // while a writer is still pending, wake the writer.
            s = Interlocked.Add(ref _state, -ReaderUnit);
            if (s == WriterPendingBit)
                _drained.Set();
            return false;
        }

        private void Acquire(CancellationToken token)
        {
            while (TryAcquire() == false)
                _writerCleared.Wait(token);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryEnterRead(out ReadHandle handle)
        {
            if (TryAcquire())
            {
                handle = new ReadHandle(this);
                return true;
            }
            handle = default;
            return false;
        }

        public ReadHandle EnterRead(CancellationToken token)
        {
            Acquire(token);
            return new ReadHandle(this);
        }

        // Holder-pattern API. Pair Acquire/TryAcquire with exactly one Release.
        public void AcquireRead(CancellationToken token) => Acquire(token);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryAcquireRead() => TryAcquire();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReleaseRead()
        {
            // Each reader pairs Add(+ReaderUnit) with Add(-ReaderUnit). A
            // negative result is the unambiguous signature of either
            // underflow (release without acquire) or count overflow; both
            // are bugs, fail loudly at the site.
            long s = Interlocked.Add(ref _state, -ReaderUnit);
            if (s < 0)
                ThrowReleaseUnderflow();
            if (s == WriterPendingBit)
                _drained.Set();
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowReleaseUnderflow() =>
            throw new InvalidOperationException("ReaderDrainLock.ReleaseRead called with no matching acquire (reader count underflow).");

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowReaderOverflow() =>
            throw new InvalidOperationException("ReaderDrainLock reader count overflowed.");

        public IDisposable EnterWrite(CancellationToken token)
        {
            // Serialize writers. Held until WriteHandle.Dispose, or until we
            // throw on timeout/cancel.
            bool gateHeld = false;
            bool publishedFlag = false;
            try
            {
                // Acquire inside the try with the ref-bool overload so an
                // async exception between Monitor.Enter returning and the
                // try-block entry cannot leak the gate.
                Monitor.Enter(_writerGate, ref gateHeld);

                _drained.Reset();
                _writerCleared.Reset();

                // Publish writer-pending and atomically observe the reader
                // count at the moment the flag becomes visible. Any reader
                // that races us sees WriterPending in its post-Add result and
                // undoes before doing protected work.
                long s = Interlocked.Or(ref _state, WriterPendingBit);
                publishedFlag = true;

                // Any non-flag bit set means readers are still active.
                if ((s & ~WriterPendingBit) != 0)
                {
                    // Note: count may transiently bump above zero here from
                    // racing readers in inspect-and-undo. Those readers see
                    // WriterPending and roll back before doing protected work,
                    // so exclusivity is preserved without waiting for the
                    // count to settle.
                    _drained.Wait(token);
                }
                return new WriteHandle(this);
            }
            catch
            {
                if (publishedFlag)
                {
                    // Roll back the writer-pending flag so readers can resume.
                    Interlocked.And(ref _state, ~WriterPendingBit);
                    _writerCleared.Set();
                    _drained.Set();
                }
                if (gateHeld)
                    Monitor.Exit(_writerGate);
                throw;
            }
        }

        internal void ExitWrite()
        {
            Interlocked.And(ref _state, ~WriterPendingBit);
            _writerCleared.Set();
            _drained.Set();
            Monitor.Exit(_writerGate);
        }

        public void Dispose()
        {
            if (_disposed)
                return;
            _disposed = true;
            _drained.Dispose();
            _writerCleared.Dispose();
        }

        // ref struct: cannot be stored in a field, captured, awaited, or boxed.
        // Always declare with `using`. Double-Dispose is a no-op.
        public ref struct ReadHandle
        {
            private ReaderDrainLock _parent;

            internal ReadHandle(ReaderDrainLock parent) { _parent = parent; }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Dispose()
            {
                ReaderDrainLock parent = _parent;
                if (parent == null)
                    return;
                _parent = null;
                parent.ReleaseRead();
            }
        }

        private sealed class WriteHandle : IDisposable
        {
            private ReaderDrainLock _parent;

            internal WriteHandle(ReaderDrainLock parent) { _parent = parent; }

            public void Dispose()
            {
                ReaderDrainLock parent = _parent;
                if (parent == null)
                    return;
                _parent = null;
                parent.ExitWrite();
            }
        }
    }
}
