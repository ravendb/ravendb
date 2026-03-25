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
    private readonly string _treeName;

    public LuceneVoronStream(Slice name, Tree.ChunkDetails[] chunksDetails, LowLevelTransaction llt) : base(name, chunksDetails, llt)
    {
        RegisterTransactionCleanup();
    }

    public unsafe LuceneVoronStream(Slice name, string treeName, byte* inlineDataPtr, int inlineDataSize, LowLevelTransaction llt) : base(name, inlineDataPtr, inlineDataSize, llt)
    {
        _treeName = treeName;
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
    public unsafe void UpdateCurrentTransaction(Transaction tx)
    {
        if (tx == null)
        {
            ThrowTransactionIsNull();
        }

        if (Llt == tx.LowLevelTransaction)
            return;

        Llt = tx.LowLevelTransaction;
        RegisterTransactionCleanup();
        if (IsInline)
        {
            var tree = tx.ReadTree(_treeName);
            byte* inlineData = null;
            if (tree == null || tree.IsInlineStream(Name, out inlineData, out _) == false)
                ThrowMissingInlineStream();
            var header = (Tree.InlineStreamHeader*)inlineData;
            InlineDataPtr = inlineData + Tree.InlineStreamHeader.SizeOf + header->Info.TagSize;
        }
        else
        {
            LastPage = default(Page);
        }
        return;
    }

    [DoesNotReturn]
    private static void ThrowTransactionIsNull()
    {
        throw new ArgumentNullException("tx");
    }

    [DoesNotReturn]
    private void ThrowMissingInlineStream() =>
        throw new InvalidOperationException($"Inline stream '{Name}' in tree '{_treeName}' not found after transaction refresh.");
}
