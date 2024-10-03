using System.Collections;
using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.RelationalDatabase.Common.Enumerators;

public sealed class TombstonesToRelationalDatabaseItems: IEnumerator<RelationalDatabaseItem>
{
    private readonly IEnumerator<Tombstone> _tombstones;
    private readonly string _collection;

    public TombstonesToRelationalDatabaseItems(IEnumerator<Tombstone> tombstones, string collection)
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

        Current = new RelationalDatabaseItem(_tombstones.Current, _collection);
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

    public RelationalDatabaseItem Current { get; private set; }
}

