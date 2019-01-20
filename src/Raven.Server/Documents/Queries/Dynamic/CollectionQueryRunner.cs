using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

namespace Raven.Server.Documents.Queries.Dynamic
{
    public class CollectionQueryRunner : AbstractQueryRunner
    {
        public const string CollectionIndexPrefix = "collection/";

        public CollectionQueryRunner(DocumentDatabase database) : base(database)
        {
        }

        public override Task<DocumentQueryResult> ExecuteQuery(IndexQueryServerSide query, DocumentsOperationContext documentsContext, long? existingResultEtag, OperationCancelToken token)
        {
            var result = new DocumentQueryResult();

            documentsContext.OpenReadTransaction();

            FillCountOfResultsAndIndexEtag(result, query.Metadata, documentsContext);

            if (query.Metadata.HasOrderByRandom == false && existingResultEtag.HasValue)
            {
                if (result.ResultEtag == existingResultEtag)
                    return Task.FromResult(DocumentQueryResult.NotModifiedResult);
            }

            ExecuteCollectionQuery(result, query, query.Metadata.CollectionName, documentsContext, token.Token);

            return Task.FromResult(result);
        }

        public override Task ExecuteStreamQuery(IndexQueryServerSide query, DocumentsOperationContext documentsContext, HttpResponse response, IStreamQueryResultWriter<Document> writer,
            OperationCancelToken token)
        {
            var result = new StreamDocumentQueryResult(response, writer, token);
            documentsContext.OpenReadTransaction();

            FillCountOfResultsAndIndexEtag(result, query.Metadata, documentsContext);

            ExecuteCollectionQuery(result, query, query.Metadata.CollectionName, documentsContext, token.Token);

            result.Flush();

            return Task.CompletedTask;
        }

        public override Task<IndexEntriesQueryResult> ExecuteIndexEntriesQuery(IndexQueryServerSide query, DocumentsOperationContext context, long? existingResultEtag, OperationCancelToken token)
        {
            throw new NotSupportedException("Collection query is handled directly by documents storage so index entries aren't created underneath");
        }

        public override Task ExecuteStreamIndexEntriesQuery(IndexQueryServerSide query, DocumentsOperationContext documentsContext, HttpResponse response,
            IStreamQueryResultWriter<BlittableJsonReaderObject> writer, OperationCancelToken token)
        {
            throw new NotSupportedException("Collection query is handled directly by documents storage so index entries aren't created underneath");
        }

        public override Task<IOperationResult> ExecuteDeleteQuery(IndexQueryServerSide query, QueryOperationOptions options, DocumentsOperationContext context, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            var runner = new CollectionRunner(Database, context, query);

            return runner.ExecuteDelete(query.Metadata.CollectionName, new CollectionOperationOptions
            {
                MaxOpsPerSecond = options.MaxOpsPerSecond
            }, onProgress, token);
        }

        public override Task<IOperationResult> ExecutePatchQuery(IndexQueryServerSide query, QueryOperationOptions options, PatchRequest patch, BlittableJsonReaderObject patchArgs, DocumentsOperationContext context, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            var runner = new CollectionRunner(Database, context, query);

            return runner.ExecutePatch(query.Metadata.CollectionName, new CollectionOperationOptions
            {
                MaxOpsPerSecond = options.MaxOpsPerSecond
            }, patch, patchArgs, onProgress, token);
        }

        private void ExecuteCollectionQuery(QueryResultServerSide<Document> resultToFill, IndexQueryServerSide query, string collection, DocumentsOperationContext context, CancellationToken cancellationToken)
        {
            using (var queryScope = query.Timings?.For(nameof(QueryTimingsScope.Names.Query)))
            {
                QueryTimingsScope gatherScope = null;
                QueryTimingsScope fillScope = null;

                if (queryScope != null && query.Metadata.Includes?.Length > 0)
                {
                    var includesScope = queryScope.For(nameof(QueryTimingsScope.Names.Includes), start: false);
                    gatherScope = includesScope.For(nameof(QueryTimingsScope.Names.Gather), start: false);
                    fillScope = includesScope.For(nameof(QueryTimingsScope.Names.Fill), start: false);
                }

                var isAllDocsCollection = collection == Constants.Documents.Collections.AllDocumentsCollection;

                // we optimize for empty queries without sorting options, appending CollectionIndexPrefix to be able to distinguish index for collection vs. physical index
                resultToFill.IndexName = isAllDocsCollection ? "AllDocs" : CollectionIndexPrefix + collection;
                resultToFill.IsStale = false;
                resultToFill.LastQueryTime = DateTime.MinValue;
                resultToFill.IndexTimestamp = DateTime.MinValue;
                resultToFill.IncludedPaths = query.Metadata.Includes;

                var fieldsToFetch = new FieldsToFetch(query, null);
                var includeDocumentsCommand = new IncludeDocumentsCommand(Database.DocumentsStorage, context, query.Metadata.Includes, fieldsToFetch.IsProjection);
                var totalResults = new Reference<int>();
                var documents = new CollectionQueryEnumerable(Database, Database.DocumentsStorage, fieldsToFetch, collection, query, queryScope, context, includeDocumentsCommand, totalResults);
                IncludeCountersCommand includeCountersCommand = null;
                if (query.Metadata.HasCounters)
                {
                    includeCountersCommand = new IncludeCountersCommand(
                        Database,
                        context,
                        query.Metadata.CounterIncludes.Counters);
                }

                try
                {
                    foreach (var document in documents)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        resultToFill.AddResult(document);

                        using (gatherScope?.Start())
                            includeDocumentsCommand.Gather(document);

                        includeCountersCommand?.Fill(document);
                    }
                }
                catch (Exception e)
                {
                    if (resultToFill.SupportsExceptionHandling == false)
                        throw;

                    resultToFill.HandleException(e);
                }

                using (fillScope?.Start())
                    includeDocumentsCommand.Fill(resultToFill.Includes);

                if (includeCountersCommand != null)
                    resultToFill.AddCounterIncludes(includeCountersCommand);

                resultToFill.TotalResults = (totalResults.Value == 0 && resultToFill.Results.Count != 0) ? -1 : totalResults.Value;
            }
        }

        private unsafe void FillCountOfResultsAndIndexEtag(QueryResultServerSide<Document> resultToFill, QueryMetadata query, DocumentsOperationContext context)
        {
            var collection = query.CollectionName;
            var buffer = stackalloc long[3];

            // If the query has include or load, it's too difficult to check the etags for just the included collections, 
            // it's easier to just show etag for all docs instead.
            if (collection == Constants.Documents.Collections.AllDocumentsCollection ||
                query.HasIncludeOrLoad)
            {
                var numberOfDocuments = Database.DocumentsStorage.GetNumberOfDocuments(context);
                buffer[0] = DocumentsStorage.ReadLastDocumentEtag(context.Transaction.InnerTransaction);
                buffer[1] = DocumentsStorage.ReadLastTombstoneEtag(context.Transaction.InnerTransaction);
                buffer[2] = numberOfDocuments;
                resultToFill.TotalResults = (int)numberOfDocuments;
            }
            else
            {
                var collectionStats = Database.DocumentsStorage.GetCollection(collection, context);

                buffer[0] = Database.DocumentsStorage.GetLastDocumentEtag(context, collection);
                buffer[1] = Database.DocumentsStorage.GetLastTombstoneEtag(context, collection);
                buffer[2] = collectionStats.Count;
            }

            resultToFill.ResultEtag = (long)Hashing.XXHash64.Calculate((byte*)buffer, sizeof(long) * 3);
            resultToFill.NodeTag = Database.ServerStore.NodeTag;
        }
    }
}
