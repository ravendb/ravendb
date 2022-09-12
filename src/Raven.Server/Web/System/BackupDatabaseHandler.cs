using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents;
using Raven.Server.Documents.Commands.OngoingTasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.System.Processors.Backups;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Web.System
{
    public sealed class BackupDatabaseHandler : RequestHandler
    {
        [RavenAction("/periodic-backup", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetPeriodicBackup()
        {
            using (var processor = new BackupDatabaseHandlerProcessorForGetPeriodicBackup(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/periodic-backup/status", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetPeriodicBackupStatus()
        {
            using (var processor = new BackupDatabaseHandlerProcessorForGetPeriodicBackupStatus(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/admin/debug/periodic-backup/timers", "GET", AuthorizationStatus.Operator)]
        public async Task GetAllPeriodicBackupsTimers()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var result = new GetPeriodicBackupTimersCommand.PeriodicBackupTimersResponse();

                foreach ((StringSegment _, Task<DocumentDatabase> task) in ServerStore.DatabasesLandlord.DatabasesCache)
                {
                    if (task.Status != TaskStatus.RanToCompletion)
                        continue;

                    var database = await task;

                    var backups = database.PeriodicBackupRunner.GetPeriodicBackupsInformation();

                    result.Timers.AddRange(backups);
                }

                result.Count = result.Timers.Count;

                var json = result.ToJson();

                context.Write(writer, json);
            }
        }
    }
}
