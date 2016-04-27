using System;

using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;

using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Queries.Results
{
    public class MapQueryResultRetriever : IQueryResultRetriever
    {
        private class FieldToFetch
        {
            public FieldToFetch(string name, bool canExtractFromIndex)
            {
                Name = name;
                CanExtractFromIndex = canExtractFromIndex;
            }

            public readonly StringSegment Name;

            public readonly bool CanExtractFromIndex;
        }

        private readonly DocumentsStorage _documentsStorage;
        private readonly DocumentsOperationContext _context;

        private readonly IndexDefinitionBase _indexDefinition;

        private readonly FieldToFetch[] _fieldsToFetch;

        private readonly bool _canExtractFromIndex;

        private readonly BlittableJsonTraverser _traverser;

        public MapQueryResultRetriever(DocumentsStorage documentsStorage, DocumentsOperationContext context, IndexDefinitionBase indexDefinition, string[] fieldsToFetch)
        {
            _documentsStorage = documentsStorage;
            _context = context;
            _indexDefinition = indexDefinition;
            _fieldsToFetch = GetFieldsToFetch(fieldsToFetch, out _canExtractFromIndex);
            _traverser = _fieldsToFetch != null && _fieldsToFetch.Length > 0 ? new BlittableJsonTraverser() : null;
        }

        private FieldToFetch[] GetFieldsToFetch(string[] fieldsToFetch, out bool canExtractFromIndex)
        {
            canExtractFromIndex = false;

            if (fieldsToFetch == null || fieldsToFetch.Length == 0)
                return null;

            var result = new FieldToFetch[fieldsToFetch.Length];
            for (var i = 0; i < fieldsToFetch.Length; i++)
            {
                var fieldToFetch = fieldsToFetch[i];

                IndexField value;
                var extract = _indexDefinition.TryGetField(fieldToFetch, out value) && value.Storage == FieldStorage.Yes;
                if (extract)
                    canExtractFromIndex = true;

                result[i] = new FieldToFetch(fieldToFetch, extract);
            }

            return result;
        }

        public Document Get(Lucene.Net.Documents.Document input)
        {
            var id = input.Get(Constants.DocumentIdFieldName);

            if (_fieldsToFetch != null && _fieldsToFetch.Length > 0)
                return GetProjection(input, id);

            return _documentsStorage.Get(_context, id);
        }

        private Document GetProjection(Lucene.Net.Documents.Document input, string id)
        {
            if (_canExtractFromIndex == false)
                return GetProjectionFromDocument(id);

            throw new NotImplementedException("We need to wait for Static indexes"); // TODO [ppekrol]
        }

        private Document GetProjectionFromDocument(string id)
        {
            var doc = _documentsStorage.Get(_context, id);
            if (doc == null)
                return null;

            var result = new DynamicJsonValue
            {
                [Constants.DocumentIdFieldName] = doc.Key
            };

            foreach (var fieldToFetch in _fieldsToFetch)
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