using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using Raven.Client.Extensions.Streams;
using Voron;
using Voron.Data;
using Voron.Data.BTrees;
using Voron.Impl;

namespace Raven.Server.Indexing;

/// <summary>
/// Wraps either a chunk-based <see cref="VoronStream"/> or an inline <see cref="UnmanagedVoronStream"/>
/// for use by Lucene index inputs. Supports cross-transaction reuse by refreshing the underlying
/// stream's transaction state (page cache reset or inline pointer update).
/// </summary>
public sealed unsafe class LuceneVoronStream
{
    private readonly VoronStream _voronStream;     // non-null when chunk-based
    private readonly UnmanagedVoronStream _inlineStream;  // non-null when inline
    private readonly string _treeName;
    private readonly string _name;
    private LowLevelTransaction _llt;
    private Stream _stream;

    // A single, reusable handler shared across every transaction this stream is bound to (used for both
    // += and -=, so detaching removes the exact same delegate instance). It is bound to the
    // CleanupTransaction method rather than a lambda on purpose: a method cannot capture enclosing locals,
    // so it is impossible to accidentally capture - and thereby pin - a LowLevelTransaction here and
    // reintroduce the retention that RavenDB-26186 set out to remove.
    private readonly Action<LowLevelTransaction> _cleanupHandler;

    /// <summary>Chunk-based stream constructor.</summary>
    public LuceneVoronStream(string name, Tree.ChunkDetails[] chunksDetails, LowLevelTransaction llt)
    {
        _name = name;
        _voronStream = new VoronStream(chunksDetails, llt);
        _stream = _voronStream;
        _llt = llt;

        _cleanupHandler = CleanupTransaction;

        _llt.Transaction.LowLevelTransaction.OnDispose += _cleanupHandler;
    }

    /// <summary>Inline (unmanaged pointer) stream constructor.</summary>
    public LuceneVoronStream(string name, string treeName, byte* inlineDataPtr, int inlineDataSize, LowLevelTransaction llt)
    {
        _name = name;
        _treeName = treeName;
        _inlineStream = new UnmanagedVoronStream(inlineDataPtr, inlineDataSize);
        _stream = _inlineStream;
        _llt = llt;

        _cleanupHandler = CleanupTransaction;

        _llt.Transaction.LowLevelTransaction.OnDispose += _cleanupHandler;
    }

    public long Position
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => _stream.Position;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        set => _stream.Position = value;
    }

    public long Length
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => _stream.Length;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int ReadByte() => _stream.ReadByte();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Read(byte[] buffer, int offset, int count) => _stream.Read(buffer, offset, count);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ReadEntireBlock(byte[] buffer, int offset, int count) => _stream.ReadEntireBlock(buffer, offset, count);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long Seek(long offset, SeekOrigin origin) => _stream.Seek(offset, origin);
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
    private void CleanupTransaction(LowLevelTransaction txBeingDisposed)
    {
        Interlocked.CompareExchange(ref _llt, null, txBeingDisposed);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void UpdateCurrentTransaction(Transaction tx)
    {
        ArgumentNullException.ThrowIfNull(tx);

        var newLlt = tx.LowLevelTransaction;

        if (_llt == newLlt)
            return;

        // Detach the handler from the old transaction. This is NOT required for correctness - the
        // compare-exchange in _cleanupHandler already makes a leftover handler a safe no-op (it only nulls
        // Llt when Llt still points to the transaction being disposed), and since the handler captures only 'this'
        // (never a LowLevelTransaction) a leftover handler does not pin the old transaction either.
        // We detach anyway to keep things tidy: handlers don't pile up on a still-live transaction's
        // OnDispose list, and a disposed transaction's list doesn't carry a dead no-op handler.
        // (If the old transaction was already disposed, Llt is null here and there is nothing to detach -
        // the handler already ran and nulled Llt.)
        var oldLlt = _llt;
        if (oldLlt != null)
            oldLlt.Transaction.LowLevelTransaction.OnDispose -= _cleanupHandler;

        _llt = newLlt;
        newLlt.Transaction.LowLevelTransaction.OnDispose += _cleanupHandler;

        if (_inlineStream != null)
        {
            var tree = tx.ReadTree(_treeName);
            byte* inlineData = null;
            if (tree == null || tree.IsInlineStream(_name, out inlineData, out _, out _) == false)
                ThrowMissingInlineStream();
            var header = (Tree.InlineStreamHeader*)inlineData;
            _inlineStream.UpdatePtr(inlineData + Tree.InlineStreamHeader.SizeOf + header->Info.TagSize);
        }
        else
        {
            _voronStream.Llt = tx.LowLevelTransaction;
            _voronStream.LastPage = default(Page);
        }
    }

    [DoesNotReturn]
    private void ThrowMissingInlineStream() =>
        throw new InvalidOperationException($"Inline stream '{_name}' in tree '{_treeName}' not found after transaction refresh.");
}
