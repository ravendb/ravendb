using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
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

internal class BatchHandlerProcessorForBulkDocs : AbstractBatchHandlerProcessorForBulkDocs<MergedBatchCommand, DatabaseRequestHandler, DocumentsOperationContext>
{
    public BatchHandlerProcessorForBulkDocs([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override async ValueTask<DynamicJsonArray> HandleTransactionAsync(JsonOperationContext context, MergedBatchCommand command)
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

    protected override async ValueTask WaitForIndexesAsync(TimeSpan timeout, List<string> specifiedIndexesQueryString, bool throwOnTimeout, string lastChangeVector, long lastTombstoneEtag,
        HashSet<string> modifiedCollections)
    {
        await WaitForIndexesAsync(RequestHandler.Database, timeout, specifiedIndexesQueryString, throwOnTimeout, lastChangeVector, lastTombstoneEtag, modifiedCollections);
    }

    public static async Task WaitForIndexesAsync(DocumentDatabase database, TimeSpan timeout, List<string> specifiedIndexesQueryString, bool throwOnTimeout, string lastChangeVector, long lastTombstoneEtag, HashSet<string> modifiedCollections)
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

        var lastEtag = lastChangeVector != null ? ChangeVectorUtils.GetEtagById(lastChangeVector, database.DbBase64Id) : 0;
        var cutoffEtag = Math.Max(lastEtag, lastTombstoneEtag);

        while (true)
        {
            var hadStaleIndexes = false;

            using (var context = QueryOperationContext.Allocate(database, needsServerContext))
            using (context.OpenReadTransaction())
            {
                foreach (var waitForIndexItem in indexesToWait)
                {
                    if (waitForIndexItem.Index.IsStale(context, cutoffEtag) == false)
                        continue;

                    hadStaleIndexes = true;

                    await waitForIndexItem.WaitForIndexing.WaitForIndexingAsync(waitForIndexItem.IndexBatchAwaiter);

                    if (waitForIndexItem.WaitForIndexing.TimeoutExceeded)
                    {
                        if (throwOnTimeout == false)
                            return;

                        throw new TimeoutException(
                            $"After waiting for {sp.Elapsed}, could not verify that {indexesToCheck.Count} " +
                            $"indexes has caught up with the changes as of etag: {cutoffEtag}");
                    }
                }
            }

            if (hadStaleIndexes == false)
                return;
        }
    }

    protected override async ValueTask WaitForReplicationAsync(TimeSpan waitForReplicasTimeout, string numberOfReplicasStr, bool throwOnTimeoutInWaitForReplicas, string lastChangeVector)
    {
        int numberOfReplicasToWaitFor;

        if (numberOfReplicasStr == "majority")
        {
            numberOfReplicasToWaitFor = RequestHandler.Database.ReplicationLoader.GetSizeOfMajority();
        }
        else
        {
            if (int.TryParse(numberOfReplicasStr, out numberOfReplicasToWaitFor) == false)
                RequestHandler.ThrowInvalidInteger("numberOfReplicasToWaitFor", numberOfReplicasStr);
        }

        var replicatedPast = await RequestHandler.Database.ReplicationLoader.WaitForReplicationAsync(
            numberOfReplicasToWaitFor,
            waitForReplicasTimeout,
            lastChangeVector);

        if (replicatedPast < numberOfReplicasToWaitFor && throwOnTimeoutInWaitForReplicas)
        {
            var message = $"Could not verify that etag {lastChangeVector} was replicated " +
                          $"to {numberOfReplicasToWaitFor} servers in {waitForReplicasTimeout}. " +
                          $"So far, it only replicated to {replicatedPast}";

            throw new TimeoutException(message);
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

    private static List<Index> GetImpactedIndexesToWaitForToBecomeNonStale(DocumentDatabase database, List<string> specifiedIndexesQueryString, HashSet<string> modifiedCollections)
    {
        var indexesToCheck = new List<Index>();

        if (specifiedIndexesQueryString.Count > 0)
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
                if (index.Collections.Contains(Constants.Documents.Collections.AllDocumentsCollection) ||
                    index.WorksOnAnyCollection(modifiedCollections))
                {
                    indexesToCheck.Add(index);
                }
            }
        }
        return indexesToCheck;
    }

    private class WaitForIndexItem
    {
        public Index Index;
        public AsyncManualResetEvent.FrozenAwaiter IndexBatchAwaiter;
        public AsyncWaitForIndexing WaitForIndexing;
    }
}
