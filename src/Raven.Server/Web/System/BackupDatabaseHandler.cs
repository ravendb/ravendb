using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Session;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Web.System
{
    public sealed class BackupDatabaseHandler : RequestHandler
    {
        [RavenAction("/periodic-backup", "GET", AuthorizationStatus.ValidUser)]
        public Task GetPeriodicBackup()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            if (TryGetAllowedDbs(name, out var _, requireAdmin: false) == false)
                return Task.CompletedTask;

            var taskId = GetLongQueryString("taskId", required: true);
            if (taskId == 0)
                throw new ArgumentException("Task ID cannot be 0");

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var databaseRecord = ServerStore.Cluster.ReadDatabase(context, name, out _);
                var periodicBackup = databaseRecord.PeriodicBackups.FirstOrDefault(x => x.TaskId == taskId);
                if (periodicBackup == null)
                    throw new InvalidOperationException($"Periodic backup task ID: {taskId} doesn't exist");

                var databaseRecordBlittable = EntityToBlittable.ConvertCommandToBlittable(periodicBackup, context);
                context.Write(writer, databaseRecordBlittable);
                writer.Flush();
            }

            return Task.CompletedTask;
        }

        [RavenAction("/periodic-backup/status", "GET", AuthorizationStatus.ValidUser)]
        public Task GetPeriodicBackupStatus()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            if (TryGetAllowedDbs(name, out var _, requireAdmin: false) == false)
                return Task.CompletedTask;

            var taskId = GetLongQueryString("taskId", required: true);
            if (taskId == 0)
                throw new ArgumentException("Task ID cannot be 0");

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            using (var statusBlittable = ServerStore.Cluster.Read(context, PeriodicBackupStatus.GenerateItemName(name, taskId.Value)))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName(nameof(GetPeriodicBackupStatusOperationResult.Status));
                writer.WriteObject(statusBlittable);
                writer.WriteEndObject();
                writer.Flush();
            }

            return Task.CompletedTask;
        }
    }
}
