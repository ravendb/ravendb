using System.Collections;
using System.Collections.Generic;
using Raven.Client.Documents.Indexes;

namespace Raven.Server.Documents.Indexes.Auto
{
    public class AutoIndexDocsEnumerator : IIndexedItemEnumerator
    {
        private readonly IndexingStatsScope _documentReadStats;
        private readonly IEnumerator<IndexItem> _itemsEnumerator;
        private readonly Document[] _results = new Document[1];

        public AutoIndexDocsEnumerator(IEnumerable<IndexItem> items, IndexingStatsScope stats)
        {
            _documentReadStats = stats.For(IndexingOperation.Map.DocumentRead, start: false);
            _itemsEnumerator = items.GetEnumerator();
        }

        public bool MoveNext(out IEnumerable resultsOfCurrentDocument)
        {
            using (_documentReadStats.Start())
            {
                var moveNext = _itemsEnumerator.MoveNext();

                _results[0] = (Document)_itemsEnumerator.Current.Item;
                resultsOfCurrentDocument = _results;

                return moveNext;
            }
        }

        public void OnError()
        {
        }

        public IndexItem Current => _itemsEnumerator.Current;

        public void Dispose()
        {
            _itemsEnumerator.Dispose();
        }
    }
}
