using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Runtime.CompilerServices;
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
    public Stream Stream { get; }

    private readonly VoronStream _voronStream;     // non-null when chunk-based
    private readonly UnmanagedVoronStream _inlineStream;  // non-null when inline
    private readonly string _treeName;
    private readonly string _name;
    private LowLevelTransaction _llt;

    /// <summary>Chunk-based stream constructor.</summary>
    public LuceneVoronStream(string name, Tree.ChunkDetails[] chunksDetails, LowLevelTransaction llt)
    {
        _name = name;
        _voronStream = new VoronStream(chunksDetails, llt);
        Stream = _voronStream;
        _llt = llt;
        RegisterTransactionCleanup();
    }

    /// <summary>Inline (unmanaged pointer) stream constructor.</summary>
    public LuceneVoronStream(string name, string treeName, byte* inlineDataPtr, int inlineDataSize, LowLevelTransaction llt)
    {
        _name = name;
        _treeName = treeName;
        _inlineStream = new UnmanagedVoronStream(inlineDataPtr, inlineDataSize);
        Stream = _inlineStream;
        _llt = llt;
        RegisterTransactionCleanup();
    }

    public long Position
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => Stream.Position;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        set => Stream.Position = value;
    }

    public long Length
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => Stream.Length;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int ReadByte() => Stream.ReadByte();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Read(byte[] buffer, int offset, int count) => Stream.Read(buffer, offset, count);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ReadEntireBlock(byte[] buffer, int offset, int count) => Stream.ReadEntireBlock(buffer, offset, count);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long Seek(long offset, SeekOrigin origin) => Stream.Seek(offset, origin);

    private void RegisterTransactionCleanup()
    {
        _llt.Transaction.LowLevelTransaction.OnDispose += _ =>
        {
            // Lucene caches these instances (via SegmentReader) per thread, so they can outlive
            // the transaction that created them. Nulling the fields here allows the GC to collect the
            // disposed LowLevelTransaction and its associated structures.
            // When the stream is reused, UpdateCurrentTransaction will set a fresh transaction.
            _llt = null;
            _inlineStream?.UpdatePtr(null);
            _voronStream?.Reset();
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void UpdateCurrentTransaction(Transaction tx)
    {
        ArgumentNullException.ThrowIfNull(tx);

        if (_llt == tx.LowLevelTransaction)
            return;

        _llt = tx.LowLevelTransaction;
        RegisterTransactionCleanup();

        if (_inlineStream != null)
        {
            var tree = tx.ReadTree(_treeName);
            byte* inlineData = null;
            if (tree == null || tree.IsInlineStream(_name, out inlineData, out _) == false)
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
