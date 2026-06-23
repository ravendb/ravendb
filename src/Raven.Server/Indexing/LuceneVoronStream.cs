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
    // A single, reusable handler shared across every transaction this stream is bound to.
    // It captures only 'this' (so it can write the Llt field) and crucially does NOT capture any
    // LowLevelTransaction - otherwise it would pin the disposed transaction and reintroduce the
    // retention that RavenDB-26186 set out to remove. The transaction to compare against comes from
    // the OnDispose argument instead.
    private readonly Action<IPagerLevelTransactionState> _cleanupHandler;

    public LuceneVoronStream(Slice name, Tree.ChunkDetails[] chunksDetails, LowLevelTransaction llt) : base(name, chunksDetails, llt)
    {
        // LowLevelTransaction.Dispose() invokes OnDispose with itself as the argument, so the handler
        // receives the exact transaction being disposed. The compare-exchange nulls Llt only when it still
        // points to that transaction, which makes a stale handler (one whose transaction was already replaced
        // by UpdateCurrentTransaction) a safe no-op, and is race-safe against a concurrent in-flight read.
        //
        // Lucene caches VoronStream instances (via SegmentReader) per thread, so they can outlive the
        // transaction that created them. Nulling Llt on dispose lets the GC collect the disposed
        // LowLevelTransaction and its associated structures (page positions, journal references, etc.),
        // which can be substantial depending on the indexing batch size.
        // When the stream is reused, UpdateCurrentTransaction will set a fresh transaction.
        _cleanupHandler = txBeingDisposed => Interlocked.CompareExchange(ref Llt, null, (LowLevelTransaction)txBeingDisposed);

        Llt.Transaction.LowLevelTransaction.OnDispose += _cleanupHandler;
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
