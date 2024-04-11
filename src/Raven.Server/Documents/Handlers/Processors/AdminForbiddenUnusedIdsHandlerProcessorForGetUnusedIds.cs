using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors;

internal class ValidateUnusedIdsHandlerProcessorForGet : AbstractValidateUnusedIdsHandlerProcessorForGet<DatabaseRequestHandler,
    DocumentsOperationContext>
{

    public ValidateUnusedIdsHandlerProcessorForGet([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override async Task ValidateUnusedIdsOnAllNodesAsync(HashSet<string> unusedIds,
        string databaseName, CancellationToken token)
    {
        DatabaseTopology topology;

        var exSb = new StringBuilder();

        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        using (var rawRecord = ServerStore.Cluster.ReadRawDatabaseRecord(context, databaseName))
        {
            topology = rawRecord.Topology;
        }

        foreach (var id in unusedIds)
        {
            if (id == topology.DatabaseTopologyIdBase64)
                exSb.Append($"{GetPrefix()} {id} (because its the DatabaseTopologyIdBase64 of {databaseName})");

            if (id == topology.ClusterTransactionIdBase64)
                exSb.Append($"{GetPrefix()} {id} (because its the 'ClusterTransactionIdBase64' of {databaseName})");
        }

        foreach (var nodeTag in topology.AllNodes)
        {
            var cmd = new GetStatisticsOperation.GetStatisticsCommand(debugTag: "unused-database-validation", nodeTag);
            await RequestHandler.ExecuteRemoteAsync(cmd, token: token);
            var stats = cmd.Result;

            if (unusedIds.Contains(stats.DatabaseId))
                exSb.AppendLine($"{GetPrefix()} {stats.DatabaseId} (because it's the database id of '{databaseName}' on node {nodeTag})");
        }

        if (exSb.Length > 0)
            throw new InvalidOperationException("Some ids cannot be added to the 'unused ids' list" + exSb.ToString());

        string GetPrefix()
        {
            return exSb.Length == 0 ? ":" : ",";
        }
    }
}
