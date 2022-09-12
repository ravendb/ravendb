using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide.Context;
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
        using (var statusBlittable = ServerStore.Cluster.Read(context, PeriodicBackupStatus.GenerateItemName(name, taskId.Value)))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            writer.WriteStartObject();
            writer.WritePropertyName(nameof(GetPeriodicBackupStatusOperationResult.Status));
            writer.WriteObject(statusBlittable);
            writer.WriteEndObject();
        }
    }
}
