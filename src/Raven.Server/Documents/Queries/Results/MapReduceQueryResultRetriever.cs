using System;
using System.Collections.Generic;
using System.Threading;
using Lucene.Net.Search;
using Lucene.Net.Store;
using System.Text;
using Corax;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Newtonsoft.Json;
using Raven.Client.Json.Serialization.NewtonsoftJson.Internal;
using Raven.Server.Documents.Indexes;
using Sparrow;
using Constants = Raven.Client.Constants;
using IndexSearcher = Corax.IndexSearcher;

namespace Raven.Server.Documents.Queries.Results
{
    public class MapReduceQueryResultRetriever : StoredValueQueryResultRetriever
    {
        public MapReduceQueryResultRetriever(DocumentDatabase database, IndexQueryServerSide query, QueryTimingsScope queryTimings, DocumentsStorage documentsStorage, JsonOperationContext context, SearchEngineType searchEngineType, FieldsToFetch fieldsToFetch, IncludeDocumentsCommand includeDocumentsCommand, IncludeCompareExchangeValuesCommand includeCompareExchangeValuesCommand, IncludeRevisionsCommand includeRevisionsCommand)
            : base(Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName, database, query, queryTimings, documentsStorage, context, searchEngineType, fieldsToFetch, includeDocumentsCommand, includeCompareExchangeValuesCommand, includeRevisionsCommand)
        {
        }
    }

    public abstract class StoredValueQueryResultRetriever : QueryResultRetrieverBase
    {
        private readonly string _storedValueFieldName;
        private readonly JsonOperationContext _context;
        private QueryTimingsScope _storageScope;

        protected StoredValueQueryResultRetriever(string storedValueFieldName, DocumentDatabase database, IndexQueryServerSide query, QueryTimingsScope queryTimings, DocumentsStorage documentsStorage, JsonOperationContext context, SearchEngineType searchEngineType, FieldsToFetch fieldsToFetch, IncludeDocumentsCommand includeDocumentsCommand, IncludeCompareExchangeValuesCommand includeCompareExchangeValuesCommand, IncludeRevisionsCommand includeRevisionsCommand)
            : base(database, query, queryTimings, searchEngineType, fieldsToFetch, documentsStorage, context, true, includeDocumentsCommand, includeCompareExchangeValuesCommand, includeRevisionsCommand)
        {
            if (storedValueFieldName == null)
                throw new ArgumentNullException(nameof(storedValueFieldName));

            _storedValueFieldName = storedValueFieldName;
            _context = context;
        }

        protected override void ValidateFieldsToFetch(FieldsToFetch fieldsToFetch)
        {
            base.ValidateFieldsToFetch(fieldsToFetch);

            if (fieldsToFetch.Projection.MustExtractFromDocument)
                throw new InvalidQueryException($"Invalid projection behavior '{_query.ProjectionBehavior}'. You can only extract values from index.", _query.Query, _query.QueryParameters);
        }

        protected override Document LoadDocument(string id)
        {
            if (DocumentsStorage != null &&
                _context is DocumentsOperationContext ctx)
                return DocumentsStorage.Get(ctx, id);
            // can happen during some debug endpoints that should never load a document
            return null;
        }

        protected override long? GetCounter(string docId, string name)
        {
            if (DocumentsStorage != null &&
                _context is DocumentsOperationContext ctx)
                return DocumentsStorage.CountersStorage.GetCounterValue(ctx, docId, name)?.Value;
            return null;
        }

        protected override DynamicJsonValue GetCounterRaw(string docId, string name)
        {
            if (DocumentsStorage == null || !(_context is DocumentsOperationContext ctx))
                return null;

            var djv = new DynamicJsonValue();

            foreach (var partialValue in DocumentsStorage.CountersStorage.GetCounterPartialValues(ctx, docId, name))
            {
                djv[partialValue.ChangeVector] = partialValue.PartialValue;
            }

            return djv;
        }
        
        public override unsafe Document DirectGet(ref RetrieverInput retrieverInput,string id, DocumentFields fields)
        {
            BlittableJsonReaderObject result;
            if (retrieverInput.IsLuceneDocument() == false)
            {
                retrieverInput.CoraxEntry.GetFieldReaderFor(retrieverInput.KnownFields.StoredJsonPropertyOffset).Read(out var binaryValue);
                fixed (byte* ptr = &binaryValue.GetPinnableReference())
                {
                    using var temp = new BlittableJsonReaderObject(ptr, binaryValue.Length, _context);
                    result = temp.Clone(_context);
                }
            }
            else
            {
                var storedValue = retrieverInput.LuceneDocument.GetField(_storedValueFieldName).GetBinaryValue(retrieverInput.State);

                var allocation = _context.GetMemory(storedValue.Length);
                var buffer = new UnmanagedWriteBuffer(_context, allocation);
                buffer.Write(storedValue, 0, storedValue.Length);

                result = new BlittableJsonReaderObject(allocation.Address, storedValue.Length, _context, buffer);
            }

            return new Document
            {
                Data = result
            };
        }

        public override (Document Document, List<Document> List) Get(ref RetrieverInput retrieverInput, CancellationToken token)
        {
            if (FieldsToFetch.IsProjection)
                return GetProjection(ref retrieverInput, null, token);

            using (_storageScope = _storageScope?.Start() ?? RetrieverScope?.For(nameof(QueryTimingsScope.Names.Storage)))
            {
                var doc = DirectGet(ref retrieverInput, null, DocumentFields.All);

                FinishDocumentSetup(doc, retrieverInput.Score);

                return (doc, null);
            }
        }

        public override bool TryGetKeyLucene(ref RetrieverInput retrieverInput, out string key)
        {
            key = null;
            return false;
        }
        
        public override bool TryGetKeyCorax(IndexSearcher searcher, long id, out UnmanagedSpan key)
        {
            key = default;
            return false;
        }
    }
}
