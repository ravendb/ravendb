using System;

using Raven.Abstractions.Data;
using Raven.Server.ServerWide.Context;

using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Queries.Results
{
    public class MapQueryResultRetriever : IQueryResultRetriever
    {
        private readonly DocumentsStorage _documentsStorage;

        private readonly DocumentsOperationContext _context;

        private readonly FieldsToFetch _fieldsToFetch;

        private readonly BlittableJsonTraverser _traverser;

        public MapQueryResultRetriever(DocumentsStorage documentsStorage, DocumentsOperationContext context, FieldsToFetch fieldsToFetch)
        {
            _documentsStorage = documentsStorage;
            _context = context;
            _fieldsToFetch = fieldsToFetch;
            _traverser = _fieldsToFetch.IsProjection ? new BlittableJsonTraverser() : null;
        }

        public Document Get(Lucene.Net.Documents.Document input)
        {
            var id = input.Get(Constants.DocumentIdFieldName);

            if (_fieldsToFetch.IsProjection)
                return GetProjection(input, id);

            return _documentsStorage.Get(_context, id);
        }

        private Document GetProjection(Lucene.Net.Documents.Document input, string id)
        {
            if (_fieldsToFetch.AnyExtractableFromIndex == false)
                return GetProjectionFromDocument(id);

            throw new NotImplementedException("We need to wait for Static indexes"); // TODO [ppekrol]
        }

        private Document GetProjectionFromDocument(string id)
        {
            var doc = _documentsStorage.Get(_context, id);
            if (doc == null)
                return null;

            var result = new DynamicJsonValue();

            if (_fieldsToFetch.IsDistinct == false)
                result[Constants.DocumentIdFieldName] = doc.Key;

            foreach (var fieldToFetch in _fieldsToFetch.Fields.Values)
            {
                object value;
                if (_traverser.TryRead(doc.Data, fieldToFetch.Name, out value) == false)
                    continue;

                result[_traverser.GetNameFromPath(fieldToFetch.Name.Value)] = value;
            }

            doc.Data.Dispose();
            doc.Data = _context.ReadObject(result, doc.Key);

            return doc;
        }
    }
}