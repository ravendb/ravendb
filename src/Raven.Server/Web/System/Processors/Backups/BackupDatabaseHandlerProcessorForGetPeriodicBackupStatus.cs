using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Backups.Sharding;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;

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
            var dbRecord = RequestHandler.ServerStore.Cluster.ReadRawDatabaseRecord(context, name);
            if (dbRecord.IsSharded == false)
            {
                using (var statusBlittable = ServerStore.Cluster.Read(context, PeriodicBackupStatus.GenerateItemName(name, taskId.Value)))
                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName(nameof(GetPeriodicBackupStatusOperationResult.IsSharded));
                    writer.WriteBool(false);
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(GetPeriodicBackupStatusOperationResult.Status));
                    writer.WriteObject(statusBlittable);
                    writer.WriteEndObject();
                }

                return;
            }

            var statusPerShard = new List<BlittableJsonReaderObject>(dbRecord.Sharding.Shards.Length);
            for (int i = 0; i < dbRecord.Sharding.Shards.Length; i++)
            {
                var statusBlittable = ServerStore.Cluster.Read(context, PeriodicBackupStatus.GenerateItemName(ShardHelper.ToShardName(name, i), taskId.Value));
                statusPerShard.Add(statusBlittable);
            }

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName(nameof(GetPeriodicBackupStatusOperationResult.IsSharded));
                writer.WriteBool(true);
                writer.WriteComma();
                writer.WriteArray(nameof(GetShardedPeriodicBackupStatusOperationResult.Statuses), statusPerShard);
                writer.WriteEndObject();
            }

            foreach (var blittable in statusPerShard)
            {
                blittable.Dispose();
            }
        }
    }
}
