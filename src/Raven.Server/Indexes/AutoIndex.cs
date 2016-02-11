using System;
using System.Collections.Generic;

using Raven.Server.Documents;

using Document = Raven.Server.Documents.Document;

namespace Raven.Server.Indexes
{
    public class AutoIndex : MapIndex
    {
        private readonly IDictionary<string, string[]> _documentPathsByCollection = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);

        public AutoIndex(int indexId, DocumentsStorage documentsStorage)
            : base(indexId, documentsStorage)
        {
        }

        protected override Lucene.Net.Documents.Document ConvertDocument(string collection, Document document)
        {
            var documentPaths = _documentPathsByCollection[collection];
            var indexDocument = new Lucene.Net.Documents.Document();

            foreach (var field in IndexPersistance.DocumentConverter.GetFields(documentPaths, document))
                indexDocument.Add(field);

            return indexDocument;
        }
    }
}