using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Dynamic;
using System.Linq;
using System.Reflection;
using Raven.Client.Data.Indexes;
using Raven.Client.Indexing;
using Raven.Client.Linq;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
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

        private static dynamic ToDynamic(object value)
        {
            IDictionary<string, object> expando = new ExpandoObject();
            
            

            return expando as ExpandoObject;
        }

        public override IEnumerable<object> EnumerateMap(IEnumerable<Document> documents, string collection, TransactionOperationContext indexContext)
        {
            var funcs = _compiled.Maps[collection];

            if (funcs.Length == 1)
            {
                foreach (var doc in funcs[0](documents.Select(x => new DynamicDocumentObject(x)))) // TODO [ppekrol] can we reuse DynamicDocumentObject
                {
                    yield return doc;
                }

                yield break;
            }

            throw new NotSupportedException("TODO arek");

            var iterateJustOnce = new List<DynamicDocumentObject>();

            foreach (var doc in documents)
                iterateJustOnce.Add(new DynamicDocumentObject(doc));

            foreach (var func in funcs)
            {
                foreach (var doc in iterateJustOnce)
                {
                    yield return new Document();
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