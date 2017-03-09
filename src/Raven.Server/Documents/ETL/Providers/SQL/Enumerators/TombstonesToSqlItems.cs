using System.Collections;
using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.SQL.Enumerators
{
    public class TombstonesToSqlItems : IEnumerator<ToSqlItem>
    {
        private readonly IEnumerator<DocumentTombstone> _tombstones;

        public TombstonesToSqlItems(IEnumerator<DocumentTombstone> tombstones)
        {
            _tombstones = tombstones;
        }

        public bool MoveNext()
        {
            if (_tombstones.MoveNext() == false)
                return false;

            Current = new ToSqlItem(_tombstones.Current);

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

        public ToSqlItem Current { get; private set; }
    }
}