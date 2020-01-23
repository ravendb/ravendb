using System.Collections.Generic;
using Raven.Client;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Indexes.Workers
{
    public sealed class MapDocuments : MapItems
    {
        private readonly DocumentsStorage _documentsStorage;

        public MapDocuments(Index index, DocumentsStorage documentsStorage, IndexStorage indexStorage, MapReduceIndexingContext mapReduceContext, IndexingConfiguration configuration)
            : base(index, indexStorage, mapReduceContext, configuration)
        {
            _documentsStorage = documentsStorage;
        }

        protected override IEnumerable<IndexItem> GetItemsEnumerator(DocumentsOperationContext databaseContext, string collection, long lastEtag, long pageSize)
        {
            foreach (var document in GetDocumentsEnumerator(databaseContext, collection, lastEtag, pageSize))
            {
                yield return new DocumentIndexItem(document.Id, document.LowerId, document.Etag, document.LastModified, document.Data.Size, document);
            }
        }

        private IEnumerable<Document> GetDocumentsEnumerator(DocumentsOperationContext databaseContext, string collection, long lastEtag, long pageSize)
        {
            if (collection == Constants.Documents.Collections.AllDocumentsCollection)
                return _documentsStorage.GetDocumentsFrom(databaseContext, lastEtag + 1, 0, pageSize);
            return _documentsStorage.GetDocumentsFrom(databaseContext, collection, lastEtag + 1, 0, pageSize);
        }
    }
}
