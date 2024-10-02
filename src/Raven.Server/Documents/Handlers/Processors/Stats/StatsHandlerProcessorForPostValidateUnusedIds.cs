using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Stats;

internal class StatsHandlerProcessorForPostValidateUnusedIds : AbstractStatsHandlerProcessorForPostValidateUnusedIds<DatabaseRequestHandler,
    DocumentsOperationContext>
{

    public StatsHandlerProcessorForPostValidateUnusedIds([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override async Task ValidateUnusedIdsOnAllNodesAsync(HashSet<string> unusedIds,
        string databaseName, CancellationToken token)
    {
        DatabaseTopology topology;

        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        using (var rawRecord = ServerStore.Cluster.ReadRawDatabaseRecord(context, databaseName))
        {
            topology = rawRecord.Topology;
        }

        foreach (var id in unusedIds)
        {
            if (id == topology.DatabaseTopologyIdBase64)
                throw new InvalidOperationException($"{id} cannot be added to the 'unused ids' list, because its the DatabaseTopologyIdBase64 of {databaseName}");

            if (id == topology.ClusterTransactionIdBase64)
                throw new InvalidOperationException($"{id} cannot be added to the 'unused ids' list, because its the 'ClusterTransactionIdBase64' of {databaseName}");
        }

        foreach (var nodeTag in topology.AllNodes)
        {
            var cmd = new GetStatisticsOperation.GetStatisticsCommand(debugTag: "unused-database-validation", nodeTag);
            await RequestHandler.ExecuteRemoteAsync(cmd, token: token);
            var stats = cmd.Result;

            if (unusedIds.Contains(stats.DatabaseId))
                throw new InvalidOperationException($"{stats.DatabaseId} cannot be added to the 'unused ids' list, because it's the database id of '{databaseName}' on node {nodeTag}");
        }
    }
}
