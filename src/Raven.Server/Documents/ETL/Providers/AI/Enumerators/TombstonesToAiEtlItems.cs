using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Raven.Server.Documents.ETL.Providers.Raven.Enumerators;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.AI.Enumerators;

public sealed class TombstonesToAiEtlItems : IEnumerator<AiEtlItem>
{
    private readonly DocumentsOperationContext _context;
    private readonly IEnumerator<Tombstone> _tombstones;
    private readonly string _collection;
    private readonly bool _trackAttachments;
    private readonly bool _allDocs;
    
    object IEnumerator.Current => Current;
    
    public AiEtlItem Current { get; private set; }
    
    // todo
    public TombstonesToAiEtlItems(DocumentsOperationContext context, IEnumerator<Tombstone> tombstones, string collection, bool trackAttachments)
    {
        _context = context;
        _tombstones = tombstones;
        _collection = collection;

        _trackAttachments = trackAttachments;
        _allDocs = _collection == null;
    }
    
    public bool MoveNext()
    {
        if (_tombstones.MoveNext() == false)
            return false;
            
        Current = new AiEtlItem(_tombstones.Current, _collection, EtlItemType.Document);
        Current.Filtered = Filter(Current);

        return true;
    }
    
    private bool Filter(AiEtlItem item)
    {
        var tombstone = _tombstones.Current;
        if (tombstone.Flags.Contain(DocumentFlags.Artificial))
            return true;

        if (_allDocs == false)
        {
            if (tombstone.Type != Tombstone.TombstoneType.Document)
                ThrowInvalidTombstoneType(Tombstone.TombstoneType.Document, tombstone.Type);

            return false;
        }

        switch (tombstone.Type)
        {
            //case Tombstone.TombstoneType.Attachment:
            //    if (_trackAttachments == false)
            //        return true;
            //
            //    return AttachmentTombstonesToRavenEtlItems.FilterAttachment(_context, item);
            case Tombstone.TombstoneType.Document:
                return false;
            case Tombstone.TombstoneType.Revision:
            case Tombstone.TombstoneType.Counter:
                return true;

            default:
                throw new ArgumentOutOfRangeException(nameof(tombstone.Type),$"Unknown type '{tombstone.Type}'");
        }
    }
    
    [DoesNotReturn]
    public static void ThrowInvalidTombstoneType(Tombstone.TombstoneType expectedType, Tombstone.TombstoneType actualType)
    {
        throw new InvalidOperationException($"When collection is specified, tombstone must be of type '{expectedType}', but got '{actualType}'");
    }

    public void Reset()
    {
        throw new System.NotImplementedException();
    }

    public void Dispose()
    {
    }
}
