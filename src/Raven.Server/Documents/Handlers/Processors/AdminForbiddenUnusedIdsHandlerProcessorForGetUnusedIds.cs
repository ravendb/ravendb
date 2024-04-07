using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors;

internal class AdminForbiddenUnusedIdsHandlerProcessorForGetUnusedIds : AbstractAdminForbiddenUnusedIdsHandlerProcessorForGetUnusedIds<DatabaseRequestHandler,
    DocumentsOperationContext>
{
    public AdminForbiddenUnusedIdsHandlerProcessorForGetUnusedIds([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override async Task ValidateUnusedIdsOnAllNodesAsync(HashSet<string> unusedIds, Dictionary<string, string> forbiddenIds,
        string databaseName, CancellationToken token)
    {
        DatabaseTopology topology;
        ClusterTopology clusterTopology;

        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        using (var rawRecord = ServerStore.Cluster.ReadRawDatabaseRecord(context, databaseName))
        {
            topology = rawRecord.Topology;
            clusterTopology = ServerStore.GetClusterTopology(context);
        }

        foreach (var id in unusedIds)
        {
            if (id == topology.DatabaseTopologyIdBase64)
            {
                forbiddenIds.Add(id,
                    $"'DatabaseTopologyIdBase64' ({topology.DatabaseTopologyIdBase64}) cannot be added to the 'unused ids' list (of '{databaseName}').");
            }

            if (id == topology.ClusterTransactionIdBase64)
            {
                forbiddenIds.Add(id,
                    $"'ClusterTransactionIdBase64' ({topology.ClusterTransactionIdBase64}) cannot be added to the 'unused ids' list (of '{databaseName}').");
            }
        }

        var nodesUrls = topology.AllNodes.Select(clusterTopology.GetUrlFromTag).ToArray();

        using var requestExecutor = RequestExecutor.Create(nodesUrls, databaseName, RequestHandler.Server.Certificate.Certificate, DocumentConventions.Default);

        foreach (var nodeTag in topology.AllNodes)
        {
            using (requestExecutor.ContextPool.AllocateOperationContext(out var context))
            {
                var cmd = new GetStatisticsOperation.GetStatisticsCommand(debugTag: "unused-database-validation", nodeTag);
                await requestExecutor.ExecuteAsync(cmd, context, token: token);
                var stats = cmd.Result;

                if (unusedIds.Contains(stats.DatabaseId))
                {
                    forbiddenIds.Add(stats.DatabaseId,
                        $"'{stats.DatabaseId}' cannot be added to the 'unused ids' list (of '{databaseName}'), because it's the database id of '{databaseName}' on node {nodeTag}.");
                }
            }
        }
    }
}
