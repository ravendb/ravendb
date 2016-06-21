using System.Collections.Generic;
using System.Linq;
using Raven.Client.Data.Indexes;
using Raven.Client.Indexing;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.ServerWide.Context;
using Voron;

namespace Raven.Server.Documents.Indexes.Static
{
    public class StaticMapIndex : MapIndexBase<StaticMapIndexDefinition>
    {
        private readonly StaticIndexBase _compiled;

        private HandleReferences _handleReferences;

        private StaticMapIndex(int indexId, StaticMapIndexDefinition definition, StaticIndexBase compiled)
            : base(indexId, IndexType.Map, definition)
        {
            _compiled = compiled;
        }

        protected override IIndexingWork[] CreateIndexWorkExecutors()
        {
            return new IIndexingWork[]
            {
                new CleanupDeletedDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, DocumentDatabase.Configuration.Indexing, null),
                _handleReferences = new HandleReferences(this, DocumentDatabase.DocumentsStorage, _indexStorage, DocumentDatabase.Configuration.Indexing),
                new MapDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, DocumentDatabase.Configuration.Indexing, null),
            };
        }

        public override void HandleDelete(DocumentTombstone tombstone, string collection, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            _handleReferences.HandleDelete(tombstone, collection, writer, indexContext, stats);
            base.HandleDelete(tombstone, collection, writer, indexContext, stats);
        }

        public override IIndexedDocumentsEnumerator EnumerateMap(IEnumerable<Document> documents, string collection, TransactionOperationContext indexContext)
        {
            return new StaticIndexDocsEnumerator(documents, _compiled.Maps[collection], collection);
        }

        public static Index CreateNew(int indexId, IndexDefinition definition, DocumentDatabase documentDatabase)
        {
            var staticIndex = IndexCompilationCache.GetIndexInstance(definition);
            var staticMapIndexDefinition = new StaticMapIndexDefinition(definition, staticIndex.Maps.Keys.ToArray());
            var instance = new StaticMapIndex(indexId, staticMapIndexDefinition, staticIndex);
            instance.Initialize(documentDatabase);

            return instance;
        }

        public static Index Open(int indexId, StorageEnvironment environment, DocumentDatabase documentDatabase)
        {
            var staticMapIndexDefinition = StaticMapIndexDefinition.Load(environment);
            var staticIndex = IndexCompilationCache.GetIndexInstance(staticMapIndexDefinition.IndexDefinition);

            var instance = new StaticMapIndex(indexId, staticMapIndexDefinition, staticIndex);
            instance.Initialize(environment, documentDatabase);

            return instance;
        }
    }
}