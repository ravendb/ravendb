using System.Collections;
using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Relational.Enumerators;

public sealed class TombstonesToRelationalItems: IEnumerator<ToRelationalItem>
{
    private readonly IEnumerator<Tombstone> _tombstones;
    private readonly string _collection;

    public TombstonesToRelationalItems(IEnumerator<Tombstone> tombstones, string collection)
    {
        _tombstones = tombstones;
        _collection = collection;
    }

    private bool Filter()
    {
        var tombstone = _tombstones.Current;
        return tombstone.Type != Tombstone.TombstoneType.Document || tombstone.Flags.Contain(DocumentFlags.Artificial);
    }

    public bool MoveNext()
    {
        if (_tombstones.MoveNext() == false)
            return false;

        Current = new ToRelationalItem(_tombstones.Current, _collection);
        Current.Filtered = Filter();
        return true;
    }

    public void Reset()
    {
        throw new System.NotImplementedException();
    }

    object IEnumerator.Current => Current;

    public void Dispose()
    {
    }

    public ToRelationalItem Current { get; private set; }
}

