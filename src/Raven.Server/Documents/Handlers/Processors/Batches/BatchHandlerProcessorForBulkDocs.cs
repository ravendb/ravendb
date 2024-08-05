using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Extensions;
using Raven.Server.Documents.Handlers.Batches;
using Raven.Server.Documents.Handlers.Batches.Commands;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Index = Raven.Server.Documents.Indexes.Index;

namespace Raven.Server.Documents.Handlers.Processors.Batches;

internal sealed class BatchHandlerProcessorForBulkDocs : AbstractBatchHandlerProcessorForBulkDocs<MergedBatchCommand, DatabaseRequestHandler, DocumentsOperationContext>
{
    public BatchHandlerProcessorForBulkDocs([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override async ValueTask<DynamicJsonArray> HandleTransactionAsync(JsonOperationContext context, MergedBatchCommand command, IndexBatchOptions indexBatchOptions, ReplicationBatchOptions replicationBatchOptions)
    {
        try
        {
            await RequestHandler.Database.TxMerger.Enqueue(command);
        }
        catch (ConcurrencyException)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.Conflict;
            throw;
        }

        return command.Reply;
    }

    protected override async ValueTask WaitForIndexesAsync(IndexBatchOptions options, string lastChangeVector, long lastTombstoneEtag,
        HashSet<string> modifiedCollections, CancellationToken token = default)
    {
        long lastEtag = ChangeVectorUtils.GetEtagById(lastChangeVector, RequestHandler.Database.DbBase64Id);
        await WaitForIndexesAsync(RequestHandler.Database, options.WaitForIndexesTimeout, options.WaitForSpecificIndexes, options.ThrowOnTimeoutInWaitForIndexes, lastEtag, lastTombstoneEtag, modifiedCollections, token);
    }

    public static async Task WaitForIndexesAsync(DocumentDatabase database, TimeSpan timeout, string[] specifiedIndexesQueryString, bool throwOnTimeout, long lastDocumentEtag, long lastTombstoneEtag, HashSet<string> modifiedCollections, CancellationToken token)
    {
        // waitForIndexesTimeout=timespan & waitForIndexThrow=false (default true)
        // waitForSpecificIndex=specific index1 & waitForSpecificIndex=specific index 2

        if (modifiedCollections.Count == 0)
            return;

        var indexesToWait = new List<WaitForIndexItem>();

        var indexesToCheck = GetImpactedIndexesToWaitForToBecomeNonStale(database, specifiedIndexesQueryString, modifiedCollections);

        if (indexesToCheck.Count == 0)
            return;

        var sp = Stopwatch.StartNew();

        bool needsServerContext = false;

        // we take the awaiter _before_ the indexing transaction happens,
        // so if there are any changes, it will already happen to it, and we'll
        // query the index again. This is important because of:
        // https://issues.hibernatingrhinos.com/issue/RavenDB-5576
        foreach (var index in indexesToCheck)
        {
            var indexToWait = new WaitForIndexItem
            {
                Index = index,
                IndexBatchAwaiter = index.GetIndexingBatchAwaiter(),
                WaitForIndexing = new AsyncWaitForIndexing(sp, timeout, index)
            };

            indexesToWait.Add(indexToWait);

            needsServerContext |= index.Definition.HasCompareExchange;
        }

        var cutoffEtag = Math.Max(lastDocumentEtag, lastTombstoneEtag);

        while (true)
        {
            var hadStaleIndexes = false;

            using (var context = QueryOperationContext.Allocate(database, needsServerContext))
            using (context.OpenReadTransaction())
            {
                for (var i = 0; i < indexesToWait.Count; i++)
                {
                    var waitForIndexItem = indexesToWait[i];
                    if (waitForIndexItem.Index.IsStale(context, cutoffEtag) == false)
                        continue;

                    hadStaleIndexes = true;
                    await waitForIndexItem.WaitForIndexing.WaitForIndexingAsync(waitForIndexItem.IndexBatchAwaiter).WithCancellation(token);

                    if (waitForIndexItem.WaitForIndexing.TimeoutExceeded)
                    {
                        if (throwOnTimeout == false)
                            return;

                        ThrowTimeoutException(indexesToWait, i, sp, context, cutoffEtag);
                    }
                }
            }

            if (hadStaleIndexes == false)
                return;
        }
    }

    protected override async ValueTask WaitForReplicationAsync(DocumentsOperationContext context, ReplicationBatchOptions options, string lastChangeVector)
    {
        var numberOfReplicasToWaitFor = options.Majority
            ? RequestHandler.Database.ReplicationLoader.GetMinNumberOfReplicas()
            : options.NumberOfReplicasToWaitFor;

        var changeVector = context.GetChangeVector(lastChangeVector);

        var replicatedPast = await RequestHandler.Database.ReplicationLoader.WaitForReplicationAsync(
            context,
            numberOfReplicasToWaitFor,
            options.WaitForReplicasTimeout,
            changeVector);

        if (replicatedPast < numberOfReplicasToWaitFor && options.ThrowOnTimeoutInWaitForReplicas)
        {
            var message = $"Could not verify that the change vector '{lastChangeVector}' was replicated " +
                          $"to {numberOfReplicasToWaitFor} servers in {options.WaitForReplicasTimeout}. " +
                          $"So far, it only replicated to {replicatedPast}";

            throw new RavenTimeoutException(message)
            {
                FailImmediately = true
            };
        }
    }

    protected override char GetIdentityPartsSeparator() => RequestHandler.Database.IdentityPartsSeparator;

    protected override AbstractBatchCommandsReader<MergedBatchCommand, DocumentsOperationContext> GetCommandsReader() => new DatabaseBatchCommandsReader(RequestHandler, RequestHandler.Database);

    protected override AbstractClusterTransactionRequestProcessor<DatabaseRequestHandler, MergedBatchCommand> GetClusterTransactionRequestProcessor()
    {
        var topology = RequestHandler.ServerStore.LoadDatabaseTopology(RequestHandler.Database.Name);
        if (topology.Promotables.Contains(RequestHandler.ServerStore.NodeTag))
            throw new DatabaseNotRelevantException("Cluster transaction can't be handled by a promotable node.");

        return new ClusterTransactionRequestProcessor(RequestHandler, topology);
    }

    [DoesNotReturn]
    private static void ThrowTimeoutException(List<WaitForIndexItem> indexesToWait, int i, Stopwatch sp, QueryOperationContext context, long cutoffEtag)
    {
        var staleIndexes = new List<string>();
        var erroredIndexes = new List<string>();
        var pausedIndexes = new List<string>();

        for (var j = i; j < indexesToWait.Count; j++)
        {
            var index = indexesToWait[j].Index;

            if (index.State == IndexState.Error)
            {
                erroredIndexes.Add(index.Name);
            }
            else if (index.Status == IndexRunningStatus.Paused)
            {
                pausedIndexes.Add(index.Name);
            }

            if (index.IsStale(context, cutoffEtag))
            {
                staleIndexes.Add(index.Name);
            }
        }

        var errorMessage = $"After waiting for {sp.Elapsed}, could not verify that all indexes has caught up with the changes as of etag: {cutoffEtag:#,#;;0}. " +
                           $"Total relevant indexes: {indexesToWait.Count}, total stale indexes: {staleIndexes.Count} ({string.Join(", ", staleIndexes)})";

        if (erroredIndexes.Count > 0)
        {
            errorMessage += $", total errored indexes: {erroredIndexes.Count} ({string.Join(", ", erroredIndexes)})";
        }

        if (pausedIndexes.Count > 0)
        {
            errorMessage += $", total paused indexes: {pausedIndexes.Count} ({string.Join(", ", pausedIndexes)})";
        }

        throw new RavenTimeoutException(errorMessage)
        {
            FailImmediately = true
        };
    }

    private static List<Index> GetImpactedIndexesToWaitForToBecomeNonStale(DocumentDatabase database, string[] specifiedIndexesQueryString, HashSet<string> modifiedCollections)
    {
        var indexesToCheck = new List<Index>();

        if (specifiedIndexesQueryString is { Length: > 0 })
        {
            var specificIndexes = specifiedIndexesQueryString.ToHashSet();
            foreach (var index in database.IndexStore.GetIndexes())
            {
                if (specificIndexes.Contains(index.Name))
                {
                    if (index.WorksOnAnyCollection(modifiedCollections))
                        indexesToCheck.Add(index);
                }
            }
        }
        else
        {
            foreach (var index in database.IndexStore.GetIndexes())
            {
                if (index.State is IndexState.Disabled)
                    continue;

                if (index.Collections.Contains(Constants.Documents.Collections.AllDocumentsCollection) ||
                    index.WorksOnAnyCollection(modifiedCollections))
                {
                    indexesToCheck.Add(index);
                }
            }
        }
        return indexesToCheck;
    }

    private sealed class WaitForIndexItem
    {
        public Index Index;
        public AsyncManualResetEvent.FrozenAwaiter IndexBatchAwaiter;
        public AsyncWaitForIndexing WaitForIndexing;
    }
}
