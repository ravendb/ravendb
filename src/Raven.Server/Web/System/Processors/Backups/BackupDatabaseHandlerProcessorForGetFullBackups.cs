using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents.PeriodicBackup.BackupHistory;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Web.System.Processors.Backups;

internal sealed class BackupDatabaseHandlerProcessorForGetFullBackups : AbstractBackupHistoryHandlerProcessor
{
    public BackupDatabaseHandlerProcessorForGetFullBackups([NotNull] RequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool ShardNumberRequired => false;

    protected override async ValueTask HandleCurrentNodeAsync()
    {
        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            var request = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "backup-history/full-backups");
            var groups = JsonDeserializationServer.BackupHistory(request).Groups;

            using (context.OpenReadTransaction())
                BackupHistoryStorage.CompleteGroups(context, DatabaseName, groups);

            var fullBackups = new BackupHistory(DatabaseName) { Groups = groups.Where(group => group.FullBackupInHistory).ToList() };

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName(nameof(BackupHistory));
                context.Write(writer, fullBackups.ToJson());
                writer.WriteEndObject();
            }
        }
    }

    protected override async ValueTask<RavenCommand<BlittableJsonReaderObject>> CreateCommandForNodeAsync(string nodeTag, JsonOperationContext context)
    {
        var request = RequestHandler.HttpContext.Request;
        var body = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "backup-history/full-backups");

        return new GetBackupHistoryFromNodeCommand(nodeTag, $"{request.Path}{request.QueryString}", body);
    }
}
