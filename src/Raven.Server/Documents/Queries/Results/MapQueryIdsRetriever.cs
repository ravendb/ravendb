using System;
using Lucene.Net.Store;
using Raven.Client;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries.Results
{
    public class MapQueryIdsRetriever : MapQueryResultRetriever
    {
        private readonly DocumentsOperationContext _context;

        public MapQueryIdsRetriever(
            DocumentDatabase database, 
            IndexQueryServerSide query, 
            QueryTimingsScope queryTimings, 
            DocumentsStorage documentsStorage, 
            DocumentsOperationContext context, 
            FieldsToFetch fieldsToFetch) 
            : base(database, query, queryTimings, documentsStorage, context, fieldsToFetch,null)
        {
            _context = context;
        }

        public override Document Get(Lucene.Net.Documents.Document input, float score, IState state)
        {
            unsafe
            {
                if (TryGetKey(input, state, out var id) == false)
                    throw new InvalidOperationException($"Could not extract '{Constants.Documents.Indexing.Fields.DocumentIdFieldName}' from index.");

                return new Document
                {
                    Id = new LazyStringValue(id, null, id.Length, _context)
                };
            }
        }
    }
}
