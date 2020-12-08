using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Server.Documents;
using Raven.Server.Documents.PeriodicBackup;
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

            var taskId = GetLongQueryString("taskId", required: true).Value;
            if (taskId == 0)
                throw new ArgumentException("Task ID cannot be 0");

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            using (var rawRecord = ServerStore.Cluster.ReadRawDatabaseRecord(context, name))
            {
                var periodicBackup = rawRecord.GetPeriodicBackupConfiguration(taskId);
                if (periodicBackup == null)
                    throw new InvalidOperationException($"Periodic backup task ID: {taskId} doesn't exist");

                context.Write(writer, periodicBackup.ToJson());
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

        [RavenAction("/admin/debug/periodic-backup/timers", "GET", AuthorizationStatus.Operator)]
        public async Task GetAllPeriodicBackupsTimers()
        {
            var first = true;
            var count = 0;

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                WriteStartOfTimers(writer);

                foreach ((var name, Task<DocumentDatabase> task) in ServerStore.DatabasesLandlord.DatabasesCache)
                {
                    if (task.Status != TaskStatus.RanToCompletion)
                        continue;

                    var database = await task;
                    if (database.PeriodicBackupRunner.PeriodicBackups.Count == 0)
                        continue;

                    if (first == false)
                        writer.WriteComma();

                    first = false;
                    WritePeriodicBackups(database, writer, context, out int c);
                    count += c;
                }

                WriteEndOfTimers(writer, count);
            }
        }

        internal static void WriteEndOfTimers(AbstractBlittableJsonTextWriter writer, int count)
        {
            writer.WriteEndArray();
            writer.WriteComma();
            writer.WritePropertyName("TimersCount");
            writer.WriteInteger(count);
            writer.WriteEndObject();
        }

        internal static void WriteStartOfTimers(AbstractBlittableJsonTextWriter writer)
        {
            writer.WriteStartObject();
            writer.WritePropertyName("TimersList");
            writer.WriteStartArray();
        }

        internal static void WritePeriodicBackups(DocumentDatabase db, AbstractBlittableJsonTextWriter writer, JsonOperationContext context, out int count)
        {
            count = 0;
            var first = true;
            foreach (var periodicBackup in db.PeriodicBackupRunner.PeriodicBackups)
            {
                if (first == false)
                    writer.WriteComma();

                first = false;
                writer.WriteStartObject();
                writer.WritePropertyName("DatabaseName");
                writer.WriteString(db.Name);
                writer.WriteComma();
                writer.WritePropertyName(nameof(periodicBackup.Configuration.TaskId));
                writer.WriteInteger(periodicBackup.Configuration.TaskId);
                writer.WriteComma();
                writer.WritePropertyName(nameof(periodicBackup.Configuration.Name));
                writer.WriteString(periodicBackup.Configuration.Name);
                writer.WriteComma();
                writer.WritePropertyName(nameof(periodicBackup.Configuration.FullBackupFrequency));
                writer.WriteString(periodicBackup.Configuration.FullBackupFrequency);
                writer.WriteComma();
                writer.WritePropertyName(nameof(periodicBackup.Configuration.IncrementalBackupFrequency));
                writer.WriteString(periodicBackup.Configuration.IncrementalBackupFrequency);
                writer.WriteComma();
                writer.WritePropertyName(nameof(NextBackup));
                using (var nextBackup = context.ReadObject(periodicBackup.GetNextBackup().ToJson(), "nextBackup"))
                    writer.WriteObject(nextBackup);
                writer.WriteComma();
                writer.WritePropertyName(nameof(PeriodicBackup.BackupTimer.CreatedAt));
                var createdAt = periodicBackup.GetCreatedAt();
                if (createdAt.HasValue == false)
                    writer.WriteNull();
                else
                    writer.WriteDateTime(createdAt.Value, isUtc: true);
                writer.WriteComma();
                writer.WritePropertyName(nameof(PeriodicBackup.Disposed));
                writer.WriteBool(periodicBackup.Disposed);
                writer.WriteEndObject();

                count++;
            }
        }
    }
}
