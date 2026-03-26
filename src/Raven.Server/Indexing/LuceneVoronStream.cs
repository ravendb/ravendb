using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Voron;
using Voron.Data;
using Voron.Data.BTrees;
using Voron.Impl;

namespace Raven.Server.Indexing;

public sealed class LuceneVoronStream : VoronStream
{
    public LuceneVoronStream(Slice name, Tree.ChunkDetails[] chunksDetails, LowLevelTransaction llt) : base(name, chunksDetails, llt)
    {
        RegisterTransactionCleanup();
    }

    private void RegisterTransactionCleanup()
    {
        Llt.Transaction.LowLevelTransaction.OnDispose += _ =>
        {
            // Lucene caches VoronStream instances (via SegmentReader) per thread, so they can outlive
            // the transaction that created them. Nulling _llt here allows the GC to collect the
            // disposed LowLevelTransaction and its associated structures (page positions, journal
            // references, etc.), which can be substantial depending on the indexing batch size.
            // When the stream is reused, UpdateCurrentTransaction will set a fresh transaction.
            Llt = null;
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void UpdateCurrentTransaction(Transaction tx)
    {
        if (tx != null)
        {
            if (Llt == tx.LowLevelTransaction)
                return;

            Llt = tx.LowLevelTransaction;
            RegisterTransactionCleanup();
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
