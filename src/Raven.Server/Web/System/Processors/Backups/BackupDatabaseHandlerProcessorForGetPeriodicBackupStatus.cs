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

internal class BackupDatabaseHandlerProcessorForGetPeriodicBackupStatus : AbstractHandlerProcessor<RequestHandler>
{
    public BackupDatabaseHandlerProcessorForGetPeriodicBackupStatus([NotNull] RequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        var name = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

        if (await RequestHandler.CanAccessDatabaseAsync(name, requireAdmin: false, requireWrite: false) == false)
            return;

        var taskId = RequestHandler.GetLongQueryString("taskId", required: true);

        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        {
            DynamicJsonValue result = new();
            var statuses = new List<BlittableJsonReaderObject>();
            var dbRecord = RequestHandler.ServerStore.Cluster.ReadRawDatabaseRecord(context, name);

            if (dbRecord.IsSharded)
            {
                foreach (var shardNumber in dbRecord.Sharding.Shards.Keys)
                {
                    var status = ServerStore.Cluster.Read(context, PeriodicBackupStatus.GenerateItemName(ShardHelper.ToShardName(name, shardNumber), taskId.Value));
                    statuses.Add(status);
                }

                result[nameof(GetShardedPeriodicBackupStatusOperationResult.IsSharded)] = true;
                result[nameof(GetShardedPeriodicBackupStatusOperationResult.Statuses)] = statuses;
            }
            else
            {
                var status = ServerStore.Cluster.Read(context, PeriodicBackupStatus.GenerateItemName(name, taskId.Value));
                statuses.Add(status);

                result[nameof(GetPeriodicBackupStatusOperationResult.IsSharded)] = false;
                result[nameof(GetPeriodicBackupStatusOperationResult.Status)] = status;
            }

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                context.Write(writer, result);
            }

            foreach (var status in statuses)
            {
                status.Dispose();
            }
        }
    }
}
