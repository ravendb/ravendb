using System.Collections.Generic;
using System.Linq;
using Raven.Client.Data.Indexes;
using Raven.Client.Indexing;
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

        public override IEnumerable<Document> EnumerateMap(IEnumerable<Document> documents, string collection)
        {
            foreach (var map in _compiled.Maps[collection])
        {
                // ReSharper disable once PossibleMultipleEnumeration
                var enumerator = map(documents).GetEnumerator();

                while (enumerator.MoveNext())
        {
                    var current = enumerator.Current;

                    // TODO object to document donverter
                    yield return new Document(); // TODO arek
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

            var instance = new StaticMapIndex(indexId, staticMapIndexDefinition, null);
            instance.Initialize(environment, documentDatabase);

            return instance;
        }
    }
}