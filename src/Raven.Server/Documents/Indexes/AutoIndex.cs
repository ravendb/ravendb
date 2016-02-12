using System;
using System.Diagnostics;
using System.Linq;

using Raven.Server.Documents.Indexes.Auto;

using Voron;

namespace Raven.Server.Documents.Indexes
{
    public class AutoIndex : MapIndex
    {
        private readonly AutoIndexDefinition _definition;

        private readonly string[] _fields;

        private AutoIndex(int indexId, DocumentsStorage documentsStorage)
            : base(indexId, IndexType.Auto, documentsStorage)
        {
        }

        private AutoIndex(int indexId, DocumentsStorage documentsStorage, AutoIndexDefinition definition)
            : base(indexId, IndexType.Auto, documentsStorage)
        {
            _definition = definition;
            _fields = definition.MapFields.ToArray();
            Collections = new[] { definition.Collection };
        }

        protected override string[] Collections { get; }

        public static AutoIndex CreateNew(int indexId, AutoIndexDefinition definition, DocumentsStorage documentsStorage)
        {
            var instance = new AutoIndex(indexId, documentsStorage, definition);
            instance.Initialize();

            return instance;
        }

        public static AutoIndex Open(int indexId, DocumentsStorage documentsStorage, StorageEnvironment environment)
        {
            var instance = new AutoIndex(indexId, documentsStorage);
            instance.Initialize(environment);

            // TODO

            return instance;
        }

        protected override Lucene.Net.Documents.Document ConvertDocument(string collection, Document document)
        {
            Debug.Assert(string.Equals(_definition.Collection, collection, StringComparison.OrdinalIgnoreCase), "Collection does not match.");

            var indexDocument = new Lucene.Net.Documents.Document();

            foreach (var field in IndexPersistence.DocumentConverter.GetFields(_fields, document))
                indexDocument.Add(field);

            return indexDocument;
        }
    }
}