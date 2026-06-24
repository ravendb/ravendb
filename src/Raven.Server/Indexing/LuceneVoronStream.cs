using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;
using Voron;
using Voron.Data;
using Voron.Data.BTrees;
using Voron.Impl;

namespace Raven.Server.Indexing;

public sealed class LuceneVoronStream : VoronStream
{
    // A single, reusable handler shared across every transaction this stream is bound to (used for both
    // += and -=, so detaching removes the exact same delegate instance). It is bound to the
    // CleanupTransaction method rather than a lambda on purpose: a method cannot capture enclosing locals,
    // so it is impossible to accidentally capture - and thereby pin - a LowLevelTransaction here and
    // reintroduce the retention that RavenDB-26186 set out to remove.
    private readonly Action<IPagerLevelTransactionState> _cleanupHandler;

    public LuceneVoronStream(Slice name, Tree.ChunkDetails[] chunksDetails, LowLevelTransaction llt) : base(name, chunksDetails, llt)
    {
        _cleanupHandler = CleanupTransaction;

        Llt.Transaction.LowLevelTransaction.OnDispose += _cleanupHandler;
    }

    // Nulls Llt when the transaction it currently points at is disposed. LowLevelTransaction.Dispose() invokes
    // OnDispose with itself as the argument, so we receive the exact transaction being disposed; the compare-exchange
    // nulls Llt only when it still points to that transaction. That makes a stale handler (one whose transaction was
    // already replaced by UpdateCurrentTransaction) a safe no-op, and is race-safe against a concurrent in-flight read.
    //
    // Why null Llt at all: Lucene caches VoronStream instances (via SegmentReader) per thread, so they can outlive the
    // transaction that created them. Nulling Llt on dispose lets the GC collect the disposed LowLevelTransaction and its
    // associated structures (page positions, journal references, etc.), which can be substantial depending on the
    // indexing batch size. When the stream is reused, UpdateCurrentTransaction sets a fresh transaction.
    //
    // This is a method, not a lambda, so it cannot capture (and thereby pin) a LowLevelTransaction. The transaction to
    // compare against always comes from the argument, never a captured variable.
    private void CleanupTransaction(IPagerLevelTransactionState txBeingDisposed)
    {
        Interlocked.CompareExchange(ref Llt, null, (LowLevelTransaction)txBeingDisposed);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void UpdateCurrentTransaction(Transaction tx)
    {
        if (tx != null)
        {
            var newLlt = tx.LowLevelTransaction;
            if (Llt == newLlt)
                return;

            // Detach the handler from the old transaction. This is NOT required for correctness - the
            // compare-exchange in _cleanupHandler already makes a leftover handler a safe no-op (it only nulls
            // Llt when Llt still points to the transaction being disposed), and since the handler captures only 'this'
            // (never a LowLevelTransaction) a leftover handler does not pin the old transaction either.
            // We detach anyway to keep things tidy: handlers don't pile up on a still-live transaction's
            // OnDispose list, and a disposed transaction's list doesn't carry a dead no-op handler.
            // (If the old transaction was already disposed, Llt is null here and there is nothing to detach -
            // the handler already ran and nulled Llt.)
            var oldLlt = Llt;
            if (oldLlt != null)
                oldLlt.Transaction.LowLevelTransaction.OnDispose -= _cleanupHandler;

            Llt = newLlt;
            newLlt.Transaction.LowLevelTransaction.OnDispose += _cleanupHandler;
            LastPage = default(Page);
            return;
        }

        ThrowTransactionIsNull();
    }

    [DoesNotReturn]
    private static void ThrowTransactionIsNull()
    {
        throw new ArgumentNullException("tx");
    }
}
