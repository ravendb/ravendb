using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public abstract class AbstractQueryRunner
    {
        protected readonly DocumentDatabase Database;

        protected AbstractQueryRunner(DocumentDatabase database)
        {
            Database = database;
        }

        protected Index GetIndex(string indexName)
        {
            var index = Database.IndexStore.GetIndex(indexName);
            if (index == null)
                IndexDoesNotExistException.ThrowFor(indexName);

            return index;
        }

        public abstract Task<DocumentQueryResult> ExecuteQuery(IndexQueryServerSide query, DocumentsOperationContext documentsContext, long? existingResultEtag, OperationCancelToken token);

        public abstract Task ExecuteStreamQuery(IndexQueryServerSide query, DocumentsOperationContext documentsContext, HttpResponse response,
            BlittableJsonTextWriter writer, OperationCancelToken token);

        public abstract Task<IndexEntriesQueryResult> ExecuteIndexEntriesQuery(IndexQueryServerSide query, DocumentsOperationContext context, long? existingResultEtag, OperationCancelToken token);
    }
}
