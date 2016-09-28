using System;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Queries.Results
{
    public class MapQueryResultRetriever : QueryResultRetrieverBase
    {
        private readonly DocumentsStorage _documentsStorage;

        private readonly DocumentsOperationContext _context;

        public MapQueryResultRetriever(DocumentsStorage documentsStorage, DocumentsOperationContext context, FieldsToFetch fieldsToFetch)
            : base(fieldsToFetch, context)
        {
            _documentsStorage = documentsStorage;
            _context = context;
        }

        public override Document Get(Lucene.Net.Documents.Document input, float score)
        {
            string id;
            if (TryGetKey(input, out id) == false)
                throw new InvalidOperationException($"Could not extract '{Constants.Indexing.Fields.DocumentIdFieldName}' from index.");

            if (_fieldsToFetch.IsProjection || _fieldsToFetch.IsTransformation)
                return GetProjection(input, score, id);

            var doc = DirectGet(null, id);

            doc?.EnsureMetadata(score);

            return doc;
        }

        public override bool TryGetKey(Lucene.Net.Documents.Document input, out string key)
        {
            key = input.Get(Constants.Indexing.Fields.DocumentIdFieldName);
            return key != null;
        }

        protected override Document DirectGet(Lucene.Net.Documents.Document input, string id)
        {
            var doc = _documentsStorage.Get(_context, id);
            if (doc == null)
                return null;
            
            return doc;
        }
    }
}