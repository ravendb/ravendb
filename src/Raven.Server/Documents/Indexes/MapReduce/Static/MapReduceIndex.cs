using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Data.Indexes;
using Raven.Client.Indexing;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.MapReduce.Static
{
    public class MapReduceIndex : MapReduceIndexBase<MapReduceIndexDefinition>
    {
        private readonly StaticIndexBase _compiled;

        private MapReduceIndex(int indexId, MapReduceIndexDefinition definition, StaticIndexBase compiled)
            : base(indexId, IndexType.MapReduce, definition)
        {
            _compiled = compiled;
        }

        public static Index CreateNew(int indexId, IndexDefinition definition, DocumentDatabase documentDatabase)
        {
            var staticIndex = IndexCompilationCache.GetIndexInstance(definition);
            var staticMapIndexDefinition = new MapReduceIndexDefinition(definition, staticIndex.Maps.Keys.ToArray());
            var instance = new MapReduceIndex(indexId, staticMapIndexDefinition, staticIndex);
            instance.Initialize(documentDatabase);

            return instance;
        }

        public override IIndexedDocumentsEnumerator EnumerateMap(IEnumerable<Document> documents, string collection, TransactionOperationContext indexContext)
        {
            return new StaticIndexDocsEnumerator(documents, _compiled.Maps[collection], collection);
        }

        public override void HandleMap(LazyStringValue key, IEnumerable mapResults, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            //throw new System.NotImplementedException();
        }
    }
}