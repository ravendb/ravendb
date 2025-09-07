using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.PeriodicBackup.BackupHistory;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Web.System.Processors.Backups;

internal sealed class BackupDatabaseHandlerProcessorForGetBackupResult : AbstractHandlerProcessor<RequestHandler>
{
    public BackupDatabaseHandlerProcessorForGetBackupResult([NotNull] RequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        var databaseName = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("database");

        var taskId = RequestHandler.GetLongQueryString("taskId");
        var createdAtTicksAsId = RequestHandler.GetLongQueryString("id");

        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            writer.WriteStartObject();
            writer.WritePropertyName(nameof(BackupResult));

            var json = BackupHistoryStorage.GetBackupResult(context, databaseName, taskId, createdAtTicksAsId);
            writer.WriteObject(json);

            writer.WriteEndObject();
        }
    }
}
