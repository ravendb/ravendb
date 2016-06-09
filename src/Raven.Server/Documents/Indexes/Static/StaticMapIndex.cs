using System.Collections.Generic;
using System.Linq;
using Raven.Client.Data.Indexes;
using Raven.Client.Indexing;
using Raven.Server.ServerWide.Context;
using Voron;

namespace Raven.Server.Documents.Indexes.Static
{
    public class StaticMapIndex : MapIndexBase<StaticMapIndexDefinition>
    {
        private readonly StaticIndexBase _compiled;

        public StaticMapIndex(int indexId, StaticMapIndexDefinition definition, StaticIndexBase compiled)
            : base(indexId, IndexType.Map, definition)
        {
            _compiled = compiled;
        }

        public override IEnumerable<object> EnumerateMap(IEnumerable<Document> documents, string collection, TransactionOperationContext indexContext)
        {
            var indexingEnumerator = new IndexedDocumentsEnumerator(documents);

            foreach (var indexingFunc in _compiled.Maps[collection])
            {
                foreach (var doc in indexingFunc(indexingEnumerator))
                {
                    yield return doc;
                }
            }
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