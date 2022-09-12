using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Web.System.Processors.Backups;

internal class BackupDatabaseHandlerProcessorForGetPeriodicBackup : AbstractHandlerProcessor<RequestHandler>
{
    public BackupDatabaseHandlerProcessorForGetPeriodicBackup([NotNull] RequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        var name = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
        if (await RequestHandler.CanAccessDatabaseAsync(name, requireAdmin: false, requireWrite: false) == false)
            return;

        var taskId = RequestHandler.GetLongQueryString("taskId", required: true).Value;
        if (taskId == 0)
            throw new ArgumentException("Task ID cannot be 0");

        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        using (var rawRecord = ServerStore.Cluster.ReadRawDatabaseRecord(context, name))
        {
            var periodicBackup = rawRecord.GetPeriodicBackupConfiguration(taskId);
            if (periodicBackup == null)
                throw new InvalidOperationException($"Periodic backup task ID: {taskId} doesn't exist");

            context.Write(writer, periodicBackup.ToJson());
        }
    }
}
