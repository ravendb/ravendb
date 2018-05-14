using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using NCrontab.Advanced;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Session;
using Raven.Client.Util;
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

        [RavenAction("/periodic-backup/next-backup-occurrence", "GET", AuthorizationStatus.ValidUser)]
        public Task GetNextBackupOccurrence()
        {
            var backupFrequency = GetQueryStringValueAndAssertIfSingleAndNotEmpty("backupFrequency");
            CrontabSchedule crontabSchedule;
            try
            {
                // will throw if the backup frequency is invalid
                crontabSchedule = CrontabSchedule.Parse(backupFrequency);
            }
            catch (Exception e)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                using (var streamWriter = new StreamWriter(ResponseBodyStream()))
                {
                    streamWriter.Write(e.Message);
                    streamWriter.Flush();
                }
                return Task.CompletedTask;
            }

            var nextOccurrence = crontabSchedule.GetNextOccurrence(SystemTime.UtcNow.ToLocalTime());

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName(nameof(NextBackupOccurrence.Utc));
                writer.WriteDateTime(nextOccurrence.ToUniversalTime(), true);
                writer.WriteComma();
                writer.WritePropertyName(nameof(NextBackupOccurrence.ServerTime));
                writer.WriteDateTime(nextOccurrence, false);
                writer.WriteEndObject();
                writer.Flush();
            }

            return Task.CompletedTask;
        }
    }

    public class NextBackupOccurrence
    {
        public DateTime Utc { get; set; }

        public DateTime ServerTime { get; set; }
    }
}
