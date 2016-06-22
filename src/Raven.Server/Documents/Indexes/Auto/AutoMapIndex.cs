using System.Collections.Generic;
using Raven.Client.Data.Indexes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;
using Voron;

namespace Raven.Server.Documents.Indexes.Auto
{
    public class AutoMapIndex : MapIndexBase<AutoMapIndexDefinition>
    {
        private AutoMapIndex(int indexId, AutoMapIndexDefinition definition)
            : base(indexId, IndexType.AutoMap, definition)
        {
        }

        public static AutoMapIndex CreateNew(int indexId, AutoMapIndexDefinition definition, DocumentDatabase documentDatabase)
        {
            var instance = new AutoMapIndex(indexId, definition);
            instance.Initialize(documentDatabase);

            return instance;
        }

        public static AutoMapIndex Open(int indexId, StorageEnvironment environment, DocumentDatabase documentDatabase)
        {
            var definition = AutoMapIndexDefinition.Load(environment);
            var instance = new AutoMapIndex(indexId, definition);
            instance.Initialize(environment, documentDatabase);

            return instance;
        }

        public override IIndexedDocumentsEnumerator GetMapEnumerator(IEnumerable<Document> documents, string collection, TransactionOperationContext indexContext)
        {
            return new AutoIndexDocsEnumerator(documents);
        }
    }
}