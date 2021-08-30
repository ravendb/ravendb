using System;
using Lucene.Net.Store;
using Raven.Client;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Voron;

namespace Raven.Server.Documents.Queries.Results
{
    public class MapQueryResultRetriever : QueryResultRetrieverBase
    {
        private readonly DocumentsOperationContext _context;
        private QueryTimingsScope _storageScope;

        public MapQueryResultRetriever(DocumentDatabase database, IndexQueryServerSide query, QueryTimingsScope queryTimings, DocumentsStorage documentsStorage, DocumentsOperationContext context, FieldsToFetch fieldsToFetch, IncludeDocumentsCommand includeDocumentsCommand, IncludeCompareExchangeValuesCommand includeCompareExchangeValuesCommand, IncludeRevisionsCommand includeRevisionsCommand )
            : base(database, query, queryTimings, fieldsToFetch, documentsStorage, context, false, includeDocumentsCommand, includeCompareExchangeValuesCommand, includeRevisionsCommand: includeRevisionsCommand)
        {
            _context = context;
        }

        public override Document Get(ref RetrieverInput retrieverInput)
        {
            var input = retrieverInput.LuceneDocument;
            var state = retrieverInput.State;
            var scoreDoc = retrieverInput.Score;
            using (RetrieverScope?.Start())
            {
                if (retrieverInput.LuceneDocument != null && TryGetKey(ref retrieverInput, out string id) == false)
                    throw new InvalidOperationException($"Could not extract '{Constants.Documents.Indexing.Fields.DocumentIdFieldName}' from index.");
                else
                    id = retrieverInput.DocumentId;

                if (FieldsToFetch.IsProjection)
                    return GetProjection(ref retrieverInput, id);

                using (_storageScope = _storageScope?.Start() ?? RetrieverScope?.For(nameof(QueryTimingsScope.Names.Storage)))
                {
                    var doc = DirectGet(null, id, DocumentFields, state);

                    FinishDocumentSetup(doc, scoreDoc);
                    return doc;
                }
            }
        }

        public override bool TryGetKey(ref RetrieverInput retrieverInput, out string key)
        {
            //Lucene method
            key = retrieverInput.LuceneDocument.Get(Constants.Documents.Indexing.Fields.DocumentIdFieldName, retrieverInput.State);
            return key != null;
        }

        protected override Document DirectGet(Lucene.Net.Documents.Document input, string id, DocumentFields fields, IState state)
        {
            return DocumentsStorage.Get(_context, id, fields);
        }

        protected override Document LoadDocument(string id)
        {
            return DocumentsStorage.Get(_context, id);
        }

        protected override long? GetCounter(string docId, string name)
        {
            var value = DocumentsStorage.CountersStorage.GetCounterValue(_context, docId, name);
            return value?.Value;
        }

        protected override DynamicJsonValue GetCounterRaw(string docId, string name)
        {
            var djv = new DynamicJsonValue();

            foreach (var partialValue in DocumentsStorage.CountersStorage.GetCounterPartialValues(_context, docId, name))
            {
                djv[partialValue.ChangeVector] = partialValue.PartialValue;
            }

            return djv;
        }
    }
}
