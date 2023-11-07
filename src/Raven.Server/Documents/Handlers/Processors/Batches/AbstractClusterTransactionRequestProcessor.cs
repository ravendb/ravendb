using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Handlers.Batches;
using Raven.Server.Documents.Handlers.Batches.Commands;
using Raven.Server.Documents.Replication;
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
    public readonly DocumentDatabase Database;

    protected AbstractClusterTransactionRequestProcessor([NotNull] TRequestHandler requestHandler, [NotNull] DocumentDatabase database)
    {
        RequestHandler = requestHandler ?? throw new ArgumentNullException(nameof(requestHandler));
        Database = database ?? throw new ArgumentNullException(nameof(database));
    }

    protected abstract ArraySegment<BatchRequestParser.CommandData> GetParsedCommands(TBatchCommand command);

    protected abstract ClusterConfiguration GetClusterConfiguration();

    public async ValueTask<(long Index, DynamicJsonArray Results)> ProcessAsync(JsonOperationContext context, TBatchCommand command)
    {
        ArraySegment<BatchRequestParser.CommandData> parsedCommands = GetParsedCommands(command);

        var waitForIndexesTimeout = RequestHandler.GetTimeSpanQueryString("waitForIndexesTimeout", required: false);
        var waitForIndexThrow = RequestHandler.GetBoolValueQueryString("waitForIndexThrow", required: false) ?? true;
        var specifiedIndexesQueryString = RequestHandler.HttpContext.Request.Query["waitForSpecificIndex"];

        var disableAtomicDocumentWrites = RequestHandler.GetBoolValueQueryString("disableAtomicDocumentWrites", required: false) ??
                                          GetClusterConfiguration().DisableAtomicDocumentWrites;
        CheckBackwardCompatibility(ref disableAtomicDocumentWrites);

        ClusterTransactionCommand.ValidateCommands(parsedCommands, disableAtomicDocumentWrites);

        var raftRequestId = RequestHandler.GetRaftRequestIdFromQuery();

        var options =
            new ClusterTransactionCommand.ClusterTransactionOptions(taskId: raftRequestId, disableAtomicDocumentWrites,
                RequestHandler.ServerStore.Engine.CommandsVersionManager.CurrentClusterMinimalVersion)
            {
                WaitForIndexesTimeout = waitForIndexesTimeout,
                WaitForIndexThrow = waitForIndexThrow,
                SpecifiedIndexesQueryString = specifiedIndexesQueryString.Count > 0 ? specifiedIndexesQueryString.ToArray() : null
            };

        ClusterTransactionCommand clusterTransactionCommand = CreateClusterTransactionCommand(parsedCommands, options, raftRequestId);

        var clusterTransactionCommandResult = await RequestHandler.ServerStore.SendToLeaderAsync(clusterTransactionCommand);
        var index = clusterTransactionCommandResult.Index;
        using var _ = RequestHandler.ServerStore.Cluster.ClusterTransactionWaiter.CreateTask(id: options.TaskId, index, Database, out var onDatabaseCompletionTask);
        var result = clusterTransactionCommandResult.Result;
        var array = new DynamicJsonArray();

        if (result is List<ClusterTransactionCommand.ClusterTransactionErrorInfo> errors)
            ThrowClusterTransactionConcurrencyException(errors);

        var count = await GetClusterTransactionCount(result, raftRequestId, clusterTransactionCommand.DatabaseCommandsCount, onDatabaseCompletionTask);
        if (count.HasValue)
            GenerateDatabaseCommandsEvaluatedResults(clusterTransactionCommand.DatabaseCommands, index, count.Value, lastModified: Database.Time.GetUtcNow(),
                options.DisableAtomicDocumentWrites, array);

        foreach (var clusterCommands in clusterTransactionCommand.ClusterCommands)
        {
            array.Add(new DynamicJsonValue { ["Type"] = clusterCommands.Type, ["Key"] = clusterCommands.Id, ["Index"] = index });
        }

        return (clusterTransactionCommandResult.Index, array);
    }

    private void ThrowClusterTransactionConcurrencyException(List<ClusterTransactionCommand.ClusterTransactionErrorInfo> errors)
    {
        RequestHandler.HttpContext.Response.StatusCode = (int)HttpStatusCode.Conflict;
        throw new ClusterTransactionConcurrencyException($"Failed to execute cluster transaction due to the following issues: {string.Join(Environment.NewLine, errors.Select(e => e.Message))}")
        {
            ConcurrencyViolations = errors.Select(e => e.Violation).ToArray()
        };
    }

    private async Task<long?> GetClusterTransactionCount(object result, string raftRequestId, long databaseCommandsCount, Task<long?> onDatabaseCompletionTask)
    {
        if (databaseCommandsCount == 0)
            return null;

        Database.ForTestingPurposes?.AfterCommitInClusterTransaction?.Invoke();
        var count = await onDatabaseCompletionTask.WithCancellation(RequestHandler.HttpContext.RequestAborted);

        if (count.HasValue)
            return count;

        /* Failover was occurred,
           and because the ClusterTransactionCommand is already completed in the DocumentDatabase in this time,
           the task with the result is no longer exists ,so the 'onDatabaseCompletionTask' is completed task which is holding null
           (that's why the count has no value). */


        if (result != null && result is ClusterTransactionResult clusterTxResult)
        {
            // We'll try to take the count from the result of the cluster transaction command that we get from the leader.

            if (clusterTxResult.Count.HasValue == false)
                throw new InvalidOperationException("Cluster Transaction result is null, but has more then 0 database commands.");

            count = clusterTxResult.Count.Value;
        }
        else
        {
            // leader isn't updated (thats why the result is empty),
            // so we'll try to take the result from the local history log.
            using (RequestHandler.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                if (RequestHandler.ServerStore.Engine.LogHistory.TryGetResultByGuid<ClusterTransactionResult>(ctx, raftRequestId, out var clusterTxLocalResult))
                {
                    count = clusterTxLocalResult.Count;
                }
                else // the command was already deleted from the log
                {
                    throw new InvalidOperationException(
                        "Cluster-transaction was succeeded, but Leader is outdated and its results are inaccessible (the command has been already deleted from the history log).  We recommend you to update all nodes in the cluster to the last stable version.");
                }
            }
        }

        return count;
    }

    private void GenerateDatabaseCommandsEvaluatedResults(List<ClusterTransactionDataCommand> databaseCommands,
    long index, long count, DateTime lastModified, bool? disableAtomicDocumentWrites,
    DynamicJsonArray commandsResults)
    {
        if (count < 0)
            throw new InvalidOperationException($"ClusterTransactionCommand result is invalid - count lower then 0 ({count}).");

        foreach (var dataCmd in databaseCommands)
        {
            count++;
            var cv = GenerateChangeVector(index, count, disableAtomicDocumentWrites, Database.DatabaseGroupId, Database.ClusterTransactionId);

            switch (dataCmd.Type)
            {
                case CommandType.PUT:
                    commandsResults.Add(new DynamicJsonValue
                    {
                        ["Type"] = dataCmd.Type,
                        [Constants.Documents.Metadata.Id] = dataCmd.Id,
                        [Constants.Documents.Metadata.Collection] = CollectionName.GetCollectionName(dataCmd.Document),
                        [Constants.Documents.Metadata.ChangeVector] = cv,
                        [Constants.Documents.Metadata.LastModified] = lastModified,
                        [Constants.Documents.Metadata.Flags] = DocumentFlags.FromClusterTransaction.ToString()
                    });
                    break;
                case CommandType.DELETE:
                    commandsResults.Add(new DynamicJsonValue
                    {
                        [nameof(BatchRequestParser.CommandData.Id)] = dataCmd.Id,
                        [nameof(BatchRequestParser.CommandData.Type)] = nameof(CommandType.DELETE),
                        ["Deleted"] = true,
                        [nameof(BatchRequestParser.CommandData.ChangeVector)] = cv
                    });
                    break;
                default:
                    throw new InvalidOperationException($"Database command type ({dataCmd.Type}) isn't valid");
            }
        }
    }

    private static string GenerateChangeVector(long index, long count, bool? disableAtomicDocumentWrites, string databaseGroupId, string clusterTransactionId)
    {
        var cv = $"{ChangeVectorParser.RaftTag}:{count}-{databaseGroupId}";
        if (disableAtomicDocumentWrites == false)
        {
            cv += $",{ChangeVectorParser.TrxnTag}:{index}-{clusterTransactionId}";
        }
        return cv;
    }


    protected abstract ClusterTransactionCommand CreateClusterTransactionCommand(ArraySegment<BatchRequestParser.CommandData> parsedCommands,
        ClusterTransactionCommand.ClusterTransactionOptions options, string raftRequestId);

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
