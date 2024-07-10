using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Exceptions;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Handlers.Batches;
using Raven.Server.Documents.Handlers.Batches.Commands;
using Raven.Server.Rachis;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using static Raven.Server.ServerWide.Commands.ClusterTransactionCommand;
using static Raven.Server.Utils.MetricCacher.Keys;

namespace Raven.Server.Documents.Handlers.Processors.Batches;

public abstract class AbstractClusterTransactionRequestProcessor<TRequestHandler, TBatchCommand>
    where TRequestHandler : RequestHandler
    where TBatchCommand : IBatchCommand
{
    protected readonly TRequestHandler RequestHandler;

    protected AbstractClusterTransactionRequestProcessor([NotNull] TRequestHandler requestHandler)
    {
        RequestHandler = requestHandler ?? throw new ArgumentNullException(nameof(requestHandler));
    }

    protected abstract ArraySegment<BatchRequestParser.CommandData> GetParsedCommands(TBatchCommand command);

    protected abstract ClusterConfiguration GetClusterConfiguration();

    public async ValueTask<(long Index, DynamicJsonArray Results)> ProcessAsync(JsonOperationContext context, TBatchCommand command, CancellationToken token)
    {
        ArraySegment<BatchRequestParser.CommandData> parsedCommands = GetParsedCommands(command);

        var waitForIndexesTimeout = RequestHandler.GetTimeSpanQueryString("waitForIndexesTimeout", required: false);
        var waitForIndexThrow = RequestHandler.GetBoolValueQueryString("waitForIndexThrow", required: false) ?? true;
        var specifiedIndexesQueryString = RequestHandler.HttpContext.Request.Query["waitForSpecificIndex"];
        var disableAtomicDocumentWrites = RequestHandler.GetBoolValueQueryString("disableAtomicDocumentWrites", required: false) ??
                                          GetClusterConfiguration().DisableAtomicDocumentWrites;

        CheckBackwardCompatibility(ref disableAtomicDocumentWrites);

        ValidateCommands(parsedCommands, disableAtomicDocumentWrites);

        var raftRequestId = RequestHandler.GetRaftRequestIdFromQuery();

        var options =
            new ClusterTransactionOptions(taskId: raftRequestId, disableAtomicDocumentWrites,
                RequestHandler.ServerStore.Engine.CommandsVersionManager.CurrentClusterMinimalVersion)
            {
                WaitForIndexesTimeout = waitForIndexesTimeout,
                WaitForIndexThrow = waitForIndexThrow,
                SpecifiedIndexesQueryString = specifiedIndexesQueryString.Count > 0 ? specifiedIndexesQueryString.ToArray() : null
            };

        var clusterTransactionCommand = CreateClusterTransactionCommand(parsedCommands, options, raftRequestId);
        clusterTransactionCommand.Timeout = Timeout.InfiniteTimeSpan; // we rely on the http token to cancel the command

        using (RequestHandler.ServerStore.Cluster.ClusterTransactionWaiter.CreateTask(id: options.TaskId, out var tcs))
        await using (token.Register(() => tcs.TrySetCanceled()))
        {
            (long index, object result) = await RequestHandler.ServerStore.SendToLeaderAsync(clusterTransactionCommand, token);
            var array = await GetClusterTransactionDatabaseCommandsResults(result, clusterTransactionCommand.DatabaseCommandsCount, index, options, tcs.Task, token);

            foreach (var clusterCommands in clusterTransactionCommand.ClusterCommands)
            {
                array.Add(new DynamicJsonValue
                {
                    [nameof(ICommandData.Type)] = clusterCommands.Type,
                    [nameof(ICompareExchangeValue.Key)] = clusterCommands.Id,
                    [nameof(ICompareExchangeValue.Index)] = index
                });
            }

            return (index, array);
        }
    }

    private async Task<DynamicJsonArray> GetClusterTransactionDatabaseCommandsResults(object result, long databaseCommandsCount, long index, ClusterTransactionOptions options, Task<HashSet<string>> onDatabaseCompletionTask, CancellationToken token)
    {
        if (result is List<ClusterTransactionErrorInfo> errors)
            ThrowClusterTransactionConcurrencyException(errors);

        if (databaseCommandsCount == 0)
            return new DynamicJsonArray();

        RequestHandler.ServerStore.ForTestingPurposes?.AfterCommitInClusterTransaction?.Invoke();

        if (result is ClusterTransactionResult clusterTxResult)
        {
            await WaitForDatabaseCompletion(onDatabaseCompletionTask, index, options, token);
            return clusterTxResult.GeneratedResult;
        }

        // leader isn't updated (that is why the result is empty),
        // so we'll try to take the result from the local history log.
        await RequestHandler.ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, index, Timeout.InfiniteTimeSpan, token);
        using (RequestHandler.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
        using (ctx.OpenReadTransaction())
        {
            if (RequestHandler.ServerStore.Engine.LogHistory.TryGetResultByGuid<ClusterTransactionResult>(ctx, options.TaskId, out var clusterTxLocalResult))
                return clusterTxLocalResult.GeneratedResult;
        }

        throw new InvalidOperationException(
            "We are not able to verify that the cluster-wide transaction was succeeded. please consider to upgrade your leader to the latest stable version.");
    }

    public abstract Task WaitForDatabaseCompletion(Task<HashSet<string>> onDatabaseCompletionTask, long index, ClusterTransactionOptions options, CancellationToken token);

    private void ThrowClusterTransactionConcurrencyException(List<ClusterTransactionErrorInfo> errors)
    {
        RequestHandler.HttpContext.Response.StatusCode = (int)HttpStatusCode.Conflict;
        throw new ClusterTransactionConcurrencyException($"Failed to execute cluster transaction due to the following issues: {string.Join(Environment.NewLine, errors.Select(e => e.Message))}")
        {
            ConcurrencyViolations = errors.Select(e => e.Violation).ToArray()
        };
    }

    protected abstract ClusterTransactionCommand CreateClusterTransactionCommand(ArraySegment<BatchRequestParser.CommandData> parsedCommands,
        ClusterTransactionOptions options, string raftRequestId);

    private void CheckBackwardCompatibility(ref bool disableAtomicDocumentWrites)
    {
        if (disableAtomicDocumentWrites)
            return;

        if (RequestRouter.TryGetClientVersion(RequestHandler.HttpContext, out var clientVersion) == false)
        {
            disableAtomicDocumentWrites = true;
            return;
        }

        if (clientVersion.Major < 5 || (clientVersion.Major == 5 && clientVersion.Minor < 2))
        {
            disableAtomicDocumentWrites = true;
        }
    }
}
