using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries.Suggestions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

namespace Raven.Server.Documents.Queries
{
    public class InvalidQueryRunner : AbstractQueryRunner
    {
        internal const string ErrorMessage = "Dynamic queries are not supported by this database because the configuration '" + nameof(IndexingConfiguration.DisableQueryOptimizerGeneratedIndexes) + "' is set to true and the query optimizer needs an index for this query";

        public InvalidQueryRunner(DocumentDatabase database) : base(database)
        {
        }

        public override Task<DocumentQueryResult> ExecuteQuery(IndexQueryServerSide query, QueryOperationContext queryContext, long? existingResultEtag, OperationCancelToken token)
        {
            throw new NotSupportedException(ErrorMessage);
        }

        public override Task ExecuteStreamQuery(IndexQueryServerSide query, QueryOperationContext queryContext, HttpResponse response, IStreamQueryResultWriter<Document> writer,
            OperationCancelToken token)
        {
            throw new NotSupportedException(ErrorMessage);
        }

        public override Task<IndexEntriesQueryResult> ExecuteIndexEntriesQuery(IndexQueryServerSide query, QueryOperationContext queryContext, bool ignoreLimit, long? existingResultEtag, OperationCancelToken token)
        {
            throw new NotSupportedException(ErrorMessage);
        }

        public override Task ExecuteStreamIndexEntriesQuery(IndexQueryServerSide query, QueryOperationContext queryContext, HttpResponse response,
            IStreamQueryResultWriter<BlittableJsonReaderObject> writer, bool ignoreLimit, OperationCancelToken token)
        {
            throw new NotSupportedException(ErrorMessage);
        }

        public override Task<IOperationResult> ExecuteDeleteQuery(IndexQueryServerSide query, QueryOperationOptions options, QueryOperationContext queryContext, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            throw new NotSupportedException(ErrorMessage);
        }

        public override Task<IOperationResult> ExecutePatchQuery(IndexQueryServerSide query, QueryOperationOptions options, PatchRequest patch,
            BlittableJsonReaderObject patchArgs, QueryOperationContext queryContext, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            throw new NotSupportedException(ErrorMessage);
        }

        public override Task<SuggestionQueryResult> ExecuteSuggestionQuery(IndexQueryServerSide query, QueryOperationContext queryContext, long? existingResultEtag, OperationCancelToken token)
        {
            throw new NotSupportedException(ErrorMessage);
        }
    }
}
