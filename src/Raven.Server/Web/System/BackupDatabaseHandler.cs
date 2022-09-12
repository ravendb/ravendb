using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents;
using Raven.Server.Documents.Commands.OngoingTasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Web.System
{
    public sealed class BackupDatabaseHandler : RequestHandler
    {
        [RavenAction("/periodic-backup", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetPeriodicBackup()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            if (await CanAccessDatabaseAsync(name, requireAdmin: false, requireWrite: false) == false)
                return;

            var taskId = GetLongQueryString("taskId", required: true).Value;
            if (taskId == 0)
                throw new ArgumentException("Task ID cannot be 0");

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            using (var rawRecord = ServerStore.Cluster.ReadRawDatabaseRecord(context, name))
            {
                var periodicBackup = rawRecord.GetPeriodicBackupConfiguration(taskId);
                if (periodicBackup == null)
                    throw new InvalidOperationException($"Periodic backup task ID: {taskId} doesn't exist");

                context.Write(writer, periodicBackup.ToJson());
            }
        }

        [RavenAction("/periodic-backup/status", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetPeriodicBackupStatus()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            if (await CanAccessDatabaseAsync(name, requireAdmin: false, requireWrite: false) == false)
                return;

            var taskId = GetLongQueryString("taskId", required: true);

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            using (var statusBlittable = ServerStore.Cluster.Read(context, PeriodicBackupStatus.GenerateItemName(name, taskId.Value)))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName(nameof(GetPeriodicBackupStatusOperationResult.Status));
                writer.WriteObject(statusBlittable);
                writer.WriteEndObject();
            }
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
