using System.Collections;
using System.Collections.Generic;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Indexes.Auto
{
    public sealed class AutoIndexDocsEnumerator : IIndexedItemEnumerator
    {
        private readonly IndexingStatsScope _documentReadStats;
        private readonly IEnumerator<IndexItem> _itemsEnumerator;
        private readonly Document[] _results = new Document[1];
        private readonly DynamicBlittableJson _dynamicBlittableJson;

        public AutoIndexDocsEnumerator(IEnumerable<IndexItem> items, IndexingStatsScope stats, string collection)
        {
            _documentReadStats = stats.For(IndexingOperation.Map.DocumentRead, start: false);
            _itemsEnumerator = items.GetEnumerator();
            _dynamicBlittableJson = new DynamicBlittableJson();
            
            CurrentIndexingScope.Current.SetSourceCollection(collection, null);
        }

        public bool MoveNext(DocumentsOperationContext ctx, out IEnumerable resultsOfCurrentDocument, out long? etag)
        {
            using (_documentReadStats.Start())
            {
                _results[0]?.Dispose();

                var moveNext = _itemsEnumerator.MoveNext();

                var document = (Document)_itemsEnumerator.Current?.Item;
                _results[0] = document;
                etag = document?.Etag;
                resultsOfCurrentDocument = _results;

                if (moveNext)
                {
                    _dynamicBlittableJson.Set(document);
                
                    CurrentIndexingScope.Current.Source = _dynamicBlittableJson;
                }

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
