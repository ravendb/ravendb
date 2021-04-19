using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries.Suggestions;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Raven.Server.Utils.Enumerators;
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

        public override async Task<DocumentQueryResult> ExecuteQuery(IndexQueryServerSide query, QueryOperationContext queryContext, long? existingResultEtag, OperationCancelToken token)
        {
            var result = new DocumentQueryResult();

            if (queryContext.AreTransactionsOpened() == false)
                queryContext.OpenReadTransaction();

            FillCountOfResultsAndIndexEtag(result, query.Metadata, queryContext);

            if (query.Metadata.HasOrderByRandom == false && existingResultEtag.HasValue)
            {
                if (result.ResultEtag == existingResultEtag)
                    return DocumentQueryResult.NotModifiedResult;
            }

            var collection = GetCollectionName(query.Metadata.CollectionName, out var indexName);

            using (QueryRunner.MarkQueryAsRunning(indexName, query, token))
            {
                result.IndexName = indexName;

                await ExecuteCollectionQueryAsync(result, query, collection, queryContext, pulseReadingTransaction: false, token.Token);

                return result;
            }
        }

        public override async Task ExecuteStreamQuery(IndexQueryServerSide query, QueryOperationContext queryContext, HttpResponse response, IStreamQueryResultWriter<Document> writer,
            OperationCancelToken token)
        {
            var result = new StreamDocumentQueryResult(response, writer, token);

            using (queryContext.OpenReadTransaction())
            {
                FillCountOfResultsAndIndexEtag(result, query.Metadata, queryContext);

                var collection = GetCollectionName(query.Metadata.CollectionName, out var indexName);

                using (QueryRunner.MarkQueryAsRunning(indexName, query, token, true))
                {
                    result.IndexName = indexName;

                    await ExecuteCollectionQueryAsync(result, query, collection, queryContext, pulseReadingTransaction: true, token.Token);

                    result.Flush();
                }
            }
        }

        public override Task<IndexEntriesQueryResult> ExecuteIndexEntriesQuery(IndexQueryServerSide query, QueryOperationContext queryContext, long? existingResultEtag, OperationCancelToken token)
        {
            throw new NotSupportedException("Collection query is handled directly by documents storage so index entries aren't created underneath");
        }

        public override Task ExecuteStreamIndexEntriesQuery(IndexQueryServerSide query, QueryOperationContext queryContext, HttpResponse response,
            IStreamQueryResultWriter<BlittableJsonReaderObject> writer, OperationCancelToken token)
        {
            throw new NotSupportedException("Collection query is handled directly by documents storage so index entries aren't created underneath");
        }

        public override Task<IOperationResult> ExecuteDeleteQuery(IndexQueryServerSide query, QueryOperationOptions options, QueryOperationContext queryContext, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            var runner = new CollectionRunner(Database, queryContext.Documents, query);

            return runner.ExecuteDelete(query.Metadata.CollectionName, query.Start, query.PageSize, new CollectionOperationOptions
            {
                MaxOpsPerSecond = options.MaxOpsPerSecond
            }, onProgress, token);
        }

        public override Task<IOperationResult> ExecutePatchQuery(IndexQueryServerSide query, QueryOperationOptions options, PatchRequest patch, BlittableJsonReaderObject patchArgs, QueryOperationContext queryContext, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            var runner = new CollectionRunner(Database, queryContext.Documents, query);

            return runner.ExecutePatch(query.Metadata.CollectionName, query.Start, query.PageSize, new CollectionOperationOptions
            {
                MaxOpsPerSecond = options.MaxOpsPerSecond
            }, patch, patchArgs, onProgress, token);
        }

        public override Task<SuggestionQueryResult> ExecuteSuggestionQuery(IndexQueryServerSide query, QueryOperationContext queryContext, long? existingResultEtag, OperationCancelToken token)
        {
            throw new NotSupportedException("Collection query is handled directly by documents storage so suggestions aren't supported");
        }

        private async ValueTask ExecuteCollectionQueryAsync(QueryResultServerSide<Document> resultToFill, IndexQueryServerSide query, string collection, QueryOperationContext context, bool pulseReadingTransaction, CancellationToken cancellationToken)
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

                // we optimize for empty queries without sorting options, appending CollectionIndexPrefix to be able to distinguish index for collection vs. physical index
                resultToFill.IsStale = false;
                resultToFill.LastQueryTime = DateTime.MinValue;
                resultToFill.IndexTimestamp = DateTime.MinValue;
                resultToFill.IncludedPaths = query.Metadata.Includes;

                var fieldsToFetch = new FieldsToFetch(query, null);
                var includeDocumentsCommand = new IncludeDocumentsCommand(Database.DocumentsStorage, context.Documents, query.Metadata.Includes, fieldsToFetch.IsProjection);
                var includeCompareExchangeValuesCommand = IncludeCompareExchangeValuesCommand.ExternalScope(context, query.Metadata.CompareExchangeValueIncludes);

                var totalResults = new Reference<int>();

                IEnumerator<Document> enumerator;

                if (pulseReadingTransaction == false)
                {
                    var documents = new CollectionQueryEnumerable(Database, Database.DocumentsStorage, fieldsToFetch, collection, query, queryScope, context.Documents, includeDocumentsCommand, includeCompareExchangeValuesCommand, totalResults);

                    enumerator = documents.GetEnumerator();
                }
                else
                {
                    enumerator = new PulsedTransactionEnumerator<Document, CollectionQueryResultsIterationState>(context.Documents,
                        state =>
                        {
                            query.Start = state.Start;
                            query.PageSize = state.Take;

                            var documents = new CollectionQueryEnumerable(Database, Database.DocumentsStorage, fieldsToFetch, collection, query, queryScope, context.Documents, includeDocumentsCommand, includeCompareExchangeValuesCommand, totalResults);

                            return documents;
                        },
                        new CollectionQueryResultsIterationState(context.Documents, Database.Configuration.Databases.PulseReadTransactionLimit)
                        {
                            Start = query.Start,
                            Take = query.PageSize
                        });
                }

                IncludeCountersCommand includeCountersCommand = null;
                IncludeTimeSeriesCommand includeTimeSeriesCommand = null;

                if (query.Metadata.CounterIncludes != null)
                {
                    includeCountersCommand = new IncludeCountersCommand(
                        Database,
                        context.Documents,
                        query.Metadata.CounterIncludes.Counters);
                }

                if (query.Metadata.TimeSeriesIncludes != null)
                {
                    includeTimeSeriesCommand = new IncludeTimeSeriesCommand(
                        context.Documents,
                        query.Metadata.TimeSeriesIncludes.TimeSeries);
                }

                try
                {
                    using (enumerator)
                    {
                        while (enumerator.MoveNext())
                        {
                            var document = enumerator.Current;

                            cancellationToken.ThrowIfCancellationRequested();

                            await resultToFill.AddResultAsync(document, cancellationToken);

                            using (gatherScope?.Start())
                            {
                                includeDocumentsCommand.Gather(document);
                                includeCompareExchangeValuesCommand?.Gather(document);
                            }

                            includeCountersCommand?.Fill(document);

                            includeTimeSeriesCommand?.Fill(document);
                        }
                    }
                }
                catch (Exception e)
                {
                    if (resultToFill.SupportsExceptionHandling == false)
                        throw;

                    await resultToFill.HandleExceptionAsync(e, cancellationToken);
                }

                using (fillScope?.Start())
                {
                    includeDocumentsCommand.Fill(resultToFill.Includes);

                    includeCompareExchangeValuesCommand.Materialize();
                }

                if (includeCompareExchangeValuesCommand != null)
                    resultToFill.AddCompareExchangeValueIncludes(includeCompareExchangeValuesCommand);

                if (includeCountersCommand != null)
                    resultToFill.AddCounterIncludes(includeCountersCommand);

                if (includeTimeSeriesCommand != null)
                    resultToFill.AddTimeSeriesIncludes(includeTimeSeriesCommand);

                resultToFill.RegisterTimeSeriesFields(query, fieldsToFetch);

                resultToFill.TotalResults = (totalResults.Value == 0 && resultToFill.Results.Count != 0) ? -1 : totalResults.Value;
                resultToFill.TotalResults64 = resultToFill.TotalResults;

                if (query.Offset != null || query.Limit != null)
                {
                    if (resultToFill.TotalResults == -1)
                    {
                        resultToFill.CappedMaxResults = query.Limit ?? -1;
                    }
                    else
                    {
                        resultToFill.CappedMaxResults = Math.Min(
                            query.Limit ?? int.MaxValue,
                            resultToFill.TotalResults - (query.Offset ?? 0)
                        );
                    }
                }
            }
        }

        private unsafe void FillCountOfResultsAndIndexEtag(QueryResultServerSide<Document> resultToFill, QueryMetadata query, QueryOperationContext context)
        {
            var bufferSize = 3;
            var hasCounters = query.HasCounterSelect || query.CounterIncludes != null;
            var hasTimeSeries = query.HasTimeSeriesSelect || query.TimeSeriesIncludes != null;
            var hasCmpXchg = query.HasCmpXchg || query.HasCmpXchgSelect || query.HasCmpXchgIncludes;

            if (hasCounters)
                bufferSize++;
            if (hasTimeSeries)
                bufferSize++;
            if (hasCmpXchg)
                bufferSize++;

            var collection = query.CollectionName;
            var buffer = stackalloc long[bufferSize];

            // If the query has include or load, it's too difficult to check the etags for just the included collections,
            // it's easier to just show etag for all docs instead.
            if (collection == Constants.Documents.Collections.AllDocumentsCollection ||
                query.HasIncludeOrLoad)
            {
                var numberOfDocuments = Database.DocumentsStorage.GetNumberOfDocuments(context.Documents);
                buffer[0] = DocumentsStorage.ReadLastDocumentEtag(context.Documents.Transaction.InnerTransaction);
                buffer[1] = DocumentsStorage.ReadLastTombstoneEtag(context.Documents.Transaction.InnerTransaction);
                buffer[2] = numberOfDocuments;

                if (hasCounters)
                    buffer[3] = DocumentsStorage.ReadLastCountersEtag(context.Documents.Transaction.InnerTransaction);

                if (hasTimeSeries)
                    buffer[hasCounters ? 4 : 3] = DocumentsStorage.ReadLastTimeSeriesEtag(context.Documents.Transaction.InnerTransaction);

                resultToFill.TotalResults = (int)numberOfDocuments;
                resultToFill.TotalResults64 = numberOfDocuments;
            }
            else
            {
                var collectionStats = Database.DocumentsStorage.GetCollection(collection, context.Documents);
                buffer[0] = Database.DocumentsStorage.GetLastDocumentEtag(context.Documents.Transaction.InnerTransaction, collection);
                buffer[1] = Database.DocumentsStorage.GetLastTombstoneEtag(context.Documents.Transaction.InnerTransaction, collection);
                buffer[2] = collectionStats.Count;

                if (hasCounters)
                    buffer[3] = Database.DocumentsStorage.CountersStorage.GetLastCounterEtag(context.Documents, collection);

                if (hasTimeSeries)
                    buffer[hasCounters ? 4 : 3] = Database.DocumentsStorage.TimeSeriesStorage.GetLastTimeSeriesEtag(context.Documents, collection);

                resultToFill.TotalResults = (int)collectionStats.Count;
                resultToFill.TotalResults64 = collectionStats.Count;
            }

            if (hasCmpXchg)
                buffer[bufferSize - 1] = Database.ServerStore.Cluster.GetLastCompareExchangeIndexForDatabase(context.Server, Database.Name);

            resultToFill.ResultEtag = (long)Hashing.XXHash64.Calculate((byte*)buffer, sizeof(long) * (uint)bufferSize);
            resultToFill.NodeTag = Database.ServerStore.NodeTag;
        }

        private static string GetCollectionName(string collection, out string indexName)
        {
            if (string.IsNullOrEmpty(collection))
                collection = Constants.Documents.Collections.AllDocumentsCollection;

            indexName = collection == Constants.Documents.Collections.AllDocumentsCollection
                ? "AllDocs"
                : CollectionIndexPrefix + collection;

            return collection;
        }
    }
}
