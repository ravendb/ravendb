//-----------------------------------------------------------------------
// <copyright file="DatabaseBulkOperations.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.Threading;
using Raven.Abstractions;
using Raven.Database.Extensions;
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Database.Impl
{
    public class DatabaseBulkOperations
    {
        private readonly DocumentDatabase database;
        private readonly TransactionInformation transactionInformation;
        private readonly CancellationTokenSource tokenSource;
        private readonly CancellationTimeout timeout;

        public DatabaseBulkOperations(DocumentDatabase database, TransactionInformation transactionInformation, CancellationTokenSource tokenSource, CancellationTimeout timeout)
        {
            this.database = database;
            this.transactionInformation = transactionInformation;
            this.tokenSource = tokenSource;
            this.timeout = timeout;
        }

        public RavenJArray DeleteByIndex(string indexName, IndexQuery queryToDelete, BulkOperationOptions options = null, Action<BulkOperationProgress> reportProgress = null)
        {
            return PerformBulkOperation(indexName, queryToDelete, options, (docId, tx) =>
            {
                database.Documents.Delete(docId, null, tx);
                return new { Document = docId, Deleted = true };
            }, reportProgress);
        }

        public RavenJArray UpdateByIndex(string indexName, IndexQuery queryToUpdate, PatchRequest[] patchRequests, BulkOperationOptions options = null, Action<BulkOperationProgress> reportProgress = null)
        {
            return PerformBulkOperation(indexName, queryToUpdate, options, (docId, tx) =>
            {
                var patchResult = database.Patches.ApplyPatch(docId, null, patchRequests, tx);
                return new { Document = docId, Result = patchResult };
            }, reportProgress);
        }

        public RavenJArray UpdateByIndex(string indexName, IndexQuery queryToUpdate, ScriptedPatchRequest patch, BulkOperationOptions options = null, Action<BulkOperationProgress> reportProgress = null)
        {
            return PerformBulkOperation(indexName, queryToUpdate, options, (docId, tx) =>
            {
                var patchResult = database.Patches.ApplyPatch(docId, null, patch, tx);
                return new { Document = docId, Result = patchResult.Item1, Debug = patchResult.Item2 };
            }, reportProgress);
        }

        private RavenJArray PerformBulkOperation(string index, IndexQuery indexQuery, BulkOperationOptions options, Func<string, TransactionInformation, object> batchOperation, Action<BulkOperationProgress> reportProgress = null)
        {
            options = options ?? new BulkOperationOptions();
            var array = new RavenJArray();
            var bulkIndexQuery = new IndexQuery
            {
                Query = indexQuery.Query,
                Start = indexQuery.Start,
                Cutoff = indexQuery.Cutoff ?? SystemTime.UtcNow,
                WaitForNonStaleResultsAsOfNow = indexQuery.WaitForNonStaleResultsAsOfNow,
                PageSize = int.MaxValue,
                FieldsToFetch = new[] { Constants.DocumentIdFieldName },
                SortedFields = indexQuery.SortedFields,
                HighlighterPreTags = indexQuery.HighlighterPreTags,
                HighlighterPostTags = indexQuery.HighlighterPostTags,
                HighlightedFields = indexQuery.HighlightedFields,
                SortHints = indexQuery.SortHints,
                HighlighterKeyName = indexQuery.HighlighterKeyName,
                TransformerParameters = indexQuery.TransformerParameters,
                ResultsTransformer = indexQuery.ResultsTransformer
            };

            var operationProgress = new BulkOperationProgress();
            
            bool stale;
            var queryResults = database.Queries.QueryDocumentIds(index, bulkIndexQuery, tokenSource, out stale);

            operationProgress.TotalEntries = queryResults.Count;

            if (stale && options.AllowStale == false)
            {
                if (options.StaleTimeout != null)
                {
                    var staleWaitTimeout = Stopwatch.StartNew();
                    while (stale && staleWaitTimeout.Elapsed < options.StaleTimeout)
                    {
                        queryResults = database.Queries.QueryDocumentIds(index, bulkIndexQuery, tokenSource, out stale);
                        operationProgress.TotalEntries = queryResults.Count;

                        if(stale)
                            SystemTime.Wait(100);
                    }
                }
                if (stale)
                {
                    if (options.StaleTimeout != null)
                        throw new InvalidOperationException("Bulk operation cancelled because the index is stale and StaleTimout  of " + options.StaleTimeout + " passed");
                    
                    throw new InvalidOperationException("Bulk operation cancelled because the index is stale and allowStale is false");
                }
            }

            var token = tokenSource.Token;		    
            const int batchSize = 1024;
            int maxOpsPerSec = options.MaxOpsPerSec ?? int.MaxValue;

            using (var enumerator = queryResults.GetEnumerator())
            {
                var duration = Stopwatch.StartNew();
                var operations = 0;
                while (true)
                {
                    database.WorkContext.UpdateFoundWork();
                    if (timeout != null)
                        timeout.Delay();
                    var batchCount = 0;
                    var shouldWaitNow = false;
                    token.ThrowIfCancellationRequested();
                    using (database.DocumentLock.Lock())
                    {
                        database.TransactionalStorage.Batch(actions =>
                        {
                            while (batchCount < batchSize && enumerator.MoveNext())
                            {
                                token.ThrowIfCancellationRequested();

                                batchCount++;
                                operations++;

                                var result = batchOperation(enumerator.Current, transactionInformation);

                                if(options.RetrieveDetails)
                                    array.Add(RavenJObject.FromObject(result));

                                operationProgress.ProcessedEntries++;

                                reportProgress?.Invoke(operationProgress);

                                if (operations >= maxOpsPerSec && duration.ElapsedMilliseconds < 1000)
                                {
                                    shouldWaitNow = true;
                                    break;
                                }

                                if (batchCount % 16 == 0 && duration.ElapsedMilliseconds > 5000)
                                {
                                    shouldWaitNow = true;
                                    break;
                                }
                            }
                        });
                    }

                    if (shouldWaitNow)
                    {
                        SystemTime.Wait(500);
                        operations = 0;
                        duration.Restart();
                        continue;
                    }

                    if (batchCount < batchSize)
                        break;
                }
            }
            return array;
        }
    }
}
