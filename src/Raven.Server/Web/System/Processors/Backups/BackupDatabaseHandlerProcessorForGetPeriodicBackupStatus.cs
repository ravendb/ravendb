using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Backups.Sharding;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.System.Processors.Backups;

internal sealed class BackupDatabaseHandlerProcessorForGetPeriodicBackupStatus : AbstractHandlerProcessor<RequestHandler>
{
    public BackupDatabaseHandlerProcessorForGetPeriodicBackupStatus([NotNull] RequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        var name = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
        var fetchLocalStatus = RequestHandler.GetBoolValueQueryString("fetchLocalStatus", required: false) ?? false;

        if (await RequestHandler.CanAccessDatabaseAsync(name, requireAdmin: false, requireWrite: false) == false)
            return;

        var taskId = RequestHandler.GetLongQueryString("taskId", required: true);

        using (ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenReadTransaction())
        {
            List<IDisposable> toDispose = new();
            DynamicJsonValue result = new();
            var dbRecord = RequestHandler.ServerStore.Cluster.ReadRawDatabaseRecord(context, name);

            if (dbRecord.IsSharded)
            {
                DynamicJsonValue statusByShard = new();

                foreach (var shardNumber in dbRecord.Sharding.Shards.Keys)
                {
                    var dbName = ShardHelper.ToShardName(name, shardNumber);
                    var status = fetchLocalStatus
                        ? BackupUtils.GetLocalBackupStatusBlittable(ServerStore, context, dbName, taskId.Value)
                        : BackupUtils.GetBackupStatusFromClusterBlittable(ServerStore, context, dbName, taskId.Value);
                    
                    toDispose.Add(status);
                    statusByShard[shardNumber.ToString()] = status;
                }

                result[nameof(GetShardedPeriodicBackupStatusOperationResult.IsSharded)] = true;
                result[nameof(GetShardedPeriodicBackupStatusOperationResult.Statuses)] = statusByShard;
            }
            else
            {
                var status = fetchLocalStatus
                    ? BackupUtils.GetLocalBackupStatusBlittable(ServerStore, context, name, taskId.Value)
                    : BackupUtils.GetBackupStatusFromClusterBlittable(ServerStore, context, name, taskId.Value);
                toDispose.Add(status);

                result[nameof(GetPeriodicBackupStatusOperationResult.IsSharded)] = false;
                result[nameof(GetPeriodicBackupStatusOperationResult.Status)] = status;
            }

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                context.Write(writer, result);
            }

            foreach (var status in toDispose)
            {
                status?.Dispose();
            }

        }
    }
}
