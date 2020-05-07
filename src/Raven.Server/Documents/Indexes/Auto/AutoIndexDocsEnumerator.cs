using System.Collections;
using System.Collections.Generic;
using Raven.Client.Documents.Indexes;
using Raven.Server.ServerWide.Context;

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

        public bool MoveNext(DocumentsOperationContext ctx, out IEnumerable resultsOfCurrentDocument, out long? etag)
        {
            using (_documentReadStats.Start())
            {
                ctx.Transaction.ForgetAbout(_results[0]);
                _results[0]?.Dispose();

                var moveNext = _itemsEnumerator.MoveNext();

                var document = (Document)_itemsEnumerator.Current?.Item;
                _results[0] = document;
                etag = document?.Etag;
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
