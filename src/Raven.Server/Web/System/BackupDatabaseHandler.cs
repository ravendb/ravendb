using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Server.Documents;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.BackupHistory;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

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
            var first = true;
            var count = 0;

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
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

        [RavenAction("/backup-history-details", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetBackupHistoryDetails()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("database");
            var taskId = GetLongQueryString("taskId");
            var createdAtTicksAsId = GetLongQueryString("id");
            var nodeTag = GetStringValuesQueryString("nodeTag");

            var currentNodeTag = ServerStore.NodeTag;
            if (nodeTag != currentNodeTag)
                throw new ArgumentException("wrong node"); //todo: rewrite message or entire functionality
            var documentDatabase = await ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(name);

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var key = $"values/{name}/backup-history/{BackupHistoryItemType.Details}/{taskId}/{nodeTag}/{createdAtTicksAsId}";

                writer.WriteStartObject();
                writer.WritePropertyName(nameof(BackupHistoryItemType.Details));

                using (documentDatabase.ConfigurationStorage.BackupHistoryStorage.ReadBackupHistoryDetails(key, out var details))
                {
                    writer.WriteObject(details);
                }

                writer.WriteEndObject();
            }
        }

        [RavenAction("/backup-history", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetBackupHistory()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("database");
            var taskIdFromRequest = GetLongQueryString("taskId", required: false);
            var nodeTagFromRequest = GetStringValuesQueryString("nodeTag", required: false);

            var currentNodeTag = ServerStore.NodeTag;
            var documentDatabase = await ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(name);

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var isFirst = true;

                writer.WriteStartObject();
                writer.WritePropertyName(nameof(BackupHistory));
                writer.WriteStartArray();

                // Let's get all backup history entries from cluster storage for current database.
                HashSet<DateTime> clusterEntriesCreationDateTimes = new();
                Dictionary<(string, long), List<BlittableJsonReaderObject>> ecsDictionary = new();

                var prefix = BackupHistoryEntry.GenerateItemPrefix(name);
                var itemsFromClusterStorage = ServerStore.Cluster
                    .ItemsStartingWith(context, prefix, 0, long.MaxValue);

                foreach ((string _, long _, BlittableJsonReaderObject value) in itemsFromClusterStorage)
                {
                    value.TryGet(nameof(BackupHistory), out BlittableJsonReaderArray entriesFromClusterStorage);
                    foreach (var entry in entriesFromClusterStorage)
                    {
                        var ecs = (BlittableJsonReaderObject)entry;
                        ecs.TryGet(nameof(BackupHistory.FullBackup), out BlittableJsonReaderObject ecsFullBackup);
                        ecsFullBackup.TryGet(nameof(BackupHistoryEntry.TaskId), out long ecsTaskId);
                        ecsFullBackup.TryGet(nameof(BackupHistoryEntry.NodeTag), out string ecsNodeTag);

                        if (ecsDictionary.TryGetValue((ecsNodeTag, ecsTaskId), out List<BlittableJsonReaderObject> entries))
                            entries.Add(ecs);
                        else
                            ecsDictionary.Add((ecsNodeTag, ecsTaskId), new List<BlittableJsonReaderObject> { ecs });

                        // In case of unstable cluster operation, it is possible to find a backup entry both in the cluster and in the temporary storage at the same time.
                        // We need to keep track of the uniqueness of the entry to avoid duplicating them in the response.
                        if (ecsNodeTag != currentNodeTag)
                            continue;
                        ecsFullBackup.TryGet(nameof(BackupHistoryEntry.CreatedAt), out DateTime ecsFullBackupCreation);
                        clusterEntriesCreationDateTimes.Add(ecsFullBackupCreation);

                        ecs.TryGet(nameof(BackupHistory.IncrementalBackups), out BlittableJsonReaderArray ecsIncrements);
                        foreach (var increment in ecsIncrements)
                        {
                            ((BlittableJsonReaderObject)increment).TryGet(nameof(BackupHistoryEntry.CreatedAt), out DateTime ecsIncrementCreation);
                            clusterEntriesCreationDateTimes.Add(ecsIncrementCreation);
                        }
                    }
                }

                Dictionary<long, List<BlittableJsonReaderObject>> etsDictionary = new();
                using (documentDatabase.ConfigurationStorage.BackupHistoryStorage.ReadItems(out var entriesFromTemporaryStorage, BackupHistoryItemType.HistoryEntry))
                {
                    if (string.IsNullOrWhiteSpace(nodeTagFromRequest) || nodeTagFromRequest == currentNodeTag)
                        foreach (var ets in entriesFromTemporaryStorage.Values)
                        {
                            ets.TryGet(nameof(BackupHistoryEntry.TaskId), out long etsTaskId);

                            if (taskIdFromRequest.HasValue && taskIdFromRequest.Value != etsTaskId)
                                continue;

                            if (etsDictionary.TryGetValue(etsTaskId, out List<BlittableJsonReaderObject> entries))
                                entries.Add(ets);
                            else
                                etsDictionary.Add(etsTaskId, new List<BlittableJsonReaderObject> { ets });
                        }

                    foreach (var ecsWithSpecificTaskIdAndNodeTag in ecsDictionary)
                    {
                        (string NodeTag, long TaskId) ecsKey = ecsWithSpecificTaskIdAndNodeTag.Key;

                        if (taskIdFromRequest.HasValue && taskIdFromRequest.Value != ecsKey.TaskId)
                            continue;

                        if (string.IsNullOrWhiteSpace(nodeTagFromRequest) == false && nodeTagFromRequest != ecsKey.NodeTag)
                            continue;

                        for (int i = 0; i < ecsWithSpecificTaskIdAndNodeTag.Value.Count; i++)
                        {
                            var entryCs = ecsWithSpecificTaskIdAndNodeTag.Value[i];

                            if (ecsKey.NodeTag != currentNodeTag)
                            {
                                // Current entry from cluster storage refers to backup history entry from another node.
                                // We can't have anything in temporary storage for this entry. Let's write it.
                                WriteObject(entryCs);
                                continue;
                            }

                            // If we have problems with the cluster, we need to pick up entries from temporary storage.
                            if (etsDictionary.TryGetValue(ecsKey.TaskId, out var etsWithSpecificTaskId) == false)
                            {
                                // We have no entries for this TaskID in temporary storage, so let's write a current entry.
                                WriteObject(entryCs);
                                continue;
                            }

                            if (i != ecsWithSpecificTaskIdAndNodeTag.Value.Count - 1)
                            {
                                // We will write all occurrences from the Cluster Storage except the last one. 
                                WriteObject(entryCs);
                                continue;
                            }

                            // This is the last entry from Cluster Storage for this TaskID.
                            // Perhaps there are temporary storage entries worth appending to increments of this backup history entry.
                            AppendLastEntriesAndWrite(etsWithSpecificTaskId, entryCs);
                        }
                    }

                    // If we have entries in the temporary storage that did not belong to any TaskID, we should also show them in the response
                    if (string.IsNullOrWhiteSpace(nodeTagFromRequest) || nodeTagFromRequest == currentNodeTag)
                    {
                        var oddments = etsDictionary
                            .Where(odd => taskIdFromRequest.HasValue == false || taskIdFromRequest.Value == odd.Key);

                        foreach (var oddment in oddments)
                            AppendLastEntriesAndWrite(oddment.Value);
                    }
                }

                writer.WriteEndArray();
                writer.WriteEndObject();

                void WriteObject(BlittableJsonReaderObject obj)
                {
                    if (isFirst == false)
                        writer.WriteComma();

                    writer.WriteObject(obj);
                    isFirst = false;
                }

                void AppendLastEntriesAndWrite(List<BlittableJsonReaderObject> entriesToAdd, BlittableJsonReaderObject lastEntry = null)
                {
                    foreach (var entryToAdd in entriesToAdd)
                    {
                        entryToAdd.TryGet(nameof(BackupHistoryEntry.CreatedAt), out DateTime entryCreatedAt);
                        if (clusterEntriesCreationDateTimes.Contains(entryCreatedAt))
                            continue;

                        entryToAdd.TryGet(nameof(BackupHistoryEntry.IsFull), out bool entryIsFull);

                        if (entryIsFull)
                        {
                            if (lastEntry != null)
                                WriteObject(lastEntry);

                            var newEntry = new DynamicJsonValue
                            {
                                [nameof(BackupHistory.FullBackup)] = entryToAdd,
                                [nameof(BackupHistory.IncrementalBackups)] = new DynamicJsonArray()
                            };

                            lastEntry = context.ReadObject(newEntry, null);
                        }
                        else
                        {
                            lastEntry.TryGet(nameof(BackupHistory.FullBackup), out BlittableJsonReaderObject lastEntryFullBackup);
                            lastEntryFullBackup.TryGet(nameof(BackupHistoryEntry.LastFullBackup), out DateTime? lastEntryLastFullBackup);
                            entryToAdd.TryGet(nameof(BackupHistoryEntry.LastFullBackup), out DateTime? entryToAddLastFullBackup);

                            if (entryToAddLastFullBackup != lastEntryLastFullBackup)
                                continue;

                            lastEntry.TryGet(nameof(BackupHistory.IncrementalBackups), out BlittableJsonReaderArray increments);
                            increments.Modifications ??= new DynamicJsonArray();
                            increments.Modifications.Add(entryToAdd);

                            lastEntry = context.ReadObject(lastEntry, null);
                        }
                    }

                    WriteObject(lastEntry);

                    entriesToAdd[0].TryGet(nameof(BackupHistoryEntry.TaskId), out long taskId);
                    etsDictionary.Remove(taskId);
                }
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
