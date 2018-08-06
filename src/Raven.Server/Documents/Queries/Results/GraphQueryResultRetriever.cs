using System;
using System.Collections.Generic;
using Lucene.Net.Store;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Queries.Results
{
    public class GraphQueryResultRetriever : QueryResultRetrieverBase
    {
        private readonly GraphQuery _graphQuery;
        private readonly DocumentsOperationContext _context;
        private QueryTimingsScope _storageScope;

        public GraphQueryResultRetriever(GraphQuery graphQuery, DocumentDatabase database,
            IndexQueryServerSide query,
            QueryTimingsScope queryTimings,
            DocumentsStorage documentsStorage,
            DocumentsOperationContext context,
            FieldsToFetch fieldsToFetch,
            IncludeDocumentsCommand includeDocumentsCommand)
            : base(database, query, queryTimings, fieldsToFetch, documentsStorage, context, false, includeDocumentsCommand)
        {
            _graphQuery = graphQuery;
            _context = context;
        }

        public Document Get(Document doc)
        {
            return GetProjectionFromDocument(doc, null, 0, FieldsToFetch, _context, null);
        }

        public override Document Get(Lucene.Net.Documents.Document input, float score, IState state)
        {
            throw new NotSupportedException("Graph Queries do not deal with Lucene indexes.");
        }

        public override bool TryGetKey(Lucene.Net.Documents.Document document, IState state, out string key)
        {
            throw new NotSupportedException("Graph Queries do not deal with Lucene indexes.");
        }

        protected override Document DirectGet(Lucene.Net.Documents.Document input, string id, IState state) => 
            DocumentsStorage.Get(_context, id);

        protected override Document LoadDocument(string id) => DocumentsStorage.Get(_context, id);

        protected override long? GetCounter(string docId, string name)
        {
            throw new NotSupportedException("Graph Queries do not deal with Counters.");
        }

        protected override DynamicJsonValue GetCounterRaw(string docId, string name)
        {
            throw new NotSupportedException("Graph Queries do not deal with Counters.");
        }
    }
}
