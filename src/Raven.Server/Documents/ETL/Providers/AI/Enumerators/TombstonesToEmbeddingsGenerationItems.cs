using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace Raven.Server.Documents.ETL.Providers.AI.Enumerators;

public sealed class TombstonesToEmbeddingsGenerationItems : IEnumerator<EmbeddingsGenerationItem>
{
    private readonly IEnumerator<Tombstone> _tombstones;
    private readonly string _collection;
    private readonly bool _allDocs;
    
    object IEnumerator.Current => Current;
    
    public EmbeddingsGenerationItem Current { get; private set; }
    
    public TombstonesToEmbeddingsGenerationItems(IEnumerator<Tombstone> tombstones, string collection)
    {
        _tombstones = tombstones;
        _collection = collection;
        
        _allDocs = _collection == null;
    }
    
    public bool MoveNext()
    {
        if (_tombstones.MoveNext() == false)
            return false;
            
        Current = new EmbeddingsGenerationItem(_tombstones.Current, _collection, EtlItemType.Document);
        Current.Filtered = Filter(Current);

        return true;
    }
    
    private bool Filter(EmbeddingsGenerationItem _)
    {
        var tombstone = _tombstones.Current;
        if (tombstone.Flags.Contain(DocumentFlags.Artificial) && tombstone.Flags.Contain(DocumentFlags.FromResharding))
            return true;

        if (_allDocs == false)
        {
            if (tombstone.Type != Tombstone.TombstoneType.Document)
                ThrowInvalidTombstoneType(Tombstone.TombstoneType.Document, tombstone.Type);

            return false;
        }

        switch (tombstone.Type)
        {
            case Tombstone.TombstoneType.Document:
                return false;
            case Tombstone.TombstoneType.Attachment:
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
        throw new NotSupportedException();
    }

    public void Dispose()
    {
    }
}
