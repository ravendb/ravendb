using System;
using Lucene.Net.Store;
using Raven.Client;
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

        public override Document Get(Lucene.Net.Documents.Document input, float score, IState state)
        {
            string id;
            if (TryGetKey(input, state, out id) == false)
                throw new InvalidOperationException($"Could not extract '{Constants.Documents.Indexing.Fields.DocumentIdFieldName}' from index.");

            if (_fieldsToFetch.IsProjection || _fieldsToFetch.IsTransformation)
                return GetProjection(input, score, id, state);

            var doc = DirectGet(null, id, state);

            if (doc != null)
                doc.IndexScore = score;

            return doc;
        }

        public override bool TryGetKey(Lucene.Net.Documents.Document input, IState state, out string key)
        {
            key = input.Get(Constants.Documents.Indexing.Fields.DocumentIdFieldName, state);
            return key != null;
        }

        protected override Document DirectGet(Lucene.Net.Documents.Document input, string id, IState state)
        {
            var doc = _documentsStorage.Get(_context, id);
            if (doc == null)
                return null;

            return doc;
        }
    }
}