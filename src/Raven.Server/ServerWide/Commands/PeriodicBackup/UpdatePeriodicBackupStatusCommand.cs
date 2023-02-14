using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide;
using Raven.Server.Documents.PeriodicBackup.BackupHistory;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.ServerWide.Commands.PeriodicBackup
{
    public class UpdatePeriodicBackupStatusCommand : UpdateValueForDatabaseCommand
    {
        private List<string> _backupDetailsIdsToDelete;

        public PeriodicBackupStatus PeriodicBackupStatus;
        public List<BackupHistoryEntry> BackupHistoryEntries;
        public int MaxNumberOfFullBackupsInBackupHistory;

        public List<BackupHistoryEntry> CurrentAndTemporarySavedEntries
        {
            get
            {
                var createdAt = PeriodicBackupStatus.IsFull
                    ? PeriodicBackupStatus.LastFullBackup ?? PeriodicBackupStatus.Error.At
                    : PeriodicBackupStatus.LastIncrementalBackup ?? PeriodicBackupStatus.Error.At;

                var entryFromBackupStatus = new BackupHistoryEntry
                {
                    BackupType = PeriodicBackupStatus.BackupType,
                    CreatedAt = createdAt,
                    DatabaseName = DatabaseName,
                    DurationInMs = PeriodicBackupStatus.DurationInMs,
                    Error = PeriodicBackupStatus.Error?.Exception,
                    IsFull = PeriodicBackupStatus.IsFull,
                    NodeTag = PeriodicBackupStatus.NodeTag,
                    LastFullBackup = PeriodicBackupStatus.LastFullBackup,
                    TaskId = PeriodicBackupStatus.TaskId
                };

                return new List<BackupHistoryEntry>(BackupHistoryEntries) { entryFromBackupStatus };
            }
        }

        // ReSharper disable once UnusedMember.Local
        private UpdatePeriodicBackupStatusCommand()
        {
            // for deserialization
        }

        public UpdatePeriodicBackupStatusCommand(string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
        }

        public override string GetItemId()
        {
            return PeriodicBackupStatus.GenerateItemName(DatabaseName, PeriodicBackupStatus.TaskId);
        }

        protected override BlittableJsonReaderObject GetUpdatedValue(long index, RawDatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue)
        {
            return context.ReadObject(PeriodicBackupStatus.ToJson(), GetItemId());
        }

        public override object GetState()
        {
            return _backupDetailsIdsToDelete;
        }

        public override unsafe void Execute(ClusterOperationContext context, Table items, long index, RawDatabaseRecord record, RachisState state, out object result)
        {
            base.Execute(context, items, index, record, state, out result);

            CurrentAndTemporarySavedEntries.ForEach(
                backupHistoryEntryToAdd =>
                {
                    var itemKey = backupHistoryEntryToAdd.GenerateItemKey();

                    // We use the follow structure for the backup history data
                    // Each Full backup + all increments are one Backup entry: { FullBackup: {}, Increments: [] }
                    using (Slice.From(context.Allocator, itemKey, out Slice valueName))
                    using (Slice.From(context.Allocator, itemKey.ToLowerInvariant(), out Slice valueNameLowered))
                    {
                        BlittableJsonReaderObject blittable;
                        if (items.ReadByKey(valueNameLowered, out var tvr) == false)
                        {
                            // This is the first backup history entry for such TaskId on that Node.Let's create a new entry.
                            // It should be taken into account that if the incremental backup is completed first after implementing the functionality on the current server instance,
                            // we should correctly handle this case. We'll define a fake backup entry with a minimal creation date as a full backup entry.
                            var fullBackup = backupHistoryEntryToAdd.IsFull
                                ? backupHistoryEntryToAdd.ToJson()
                                : new DynamicJsonValue
                                {
                                    [nameof(BackupHistoryEntry.BackupType)] = backupHistoryEntryToAdd.BackupType,
                                    [nameof(BackupHistoryEntry.CreatedAt)] = DateTime.MinValue,
                                    [nameof(BackupHistoryEntry.DatabaseName)] = backupHistoryEntryToAdd.DatabaseName,
                                    [nameof(BackupHistoryEntry.DurationInMs)] = 0,
                                    [nameof(BackupHistoryEntry.Error)] = null,
                                    [nameof(BackupHistoryEntry.IsFull)] = true,
                                    [nameof(BackupHistoryEntry.NodeTag)] = backupHistoryEntryToAdd.NodeTag,
                                    [nameof(BackupHistoryEntry.LastFullBackup)] = backupHistoryEntryToAdd.LastFullBackup,
                                    [nameof(BackupHistoryEntry.TaskId)] = backupHistoryEntryToAdd.TaskId,
                                };

                            var increments = backupHistoryEntryToAdd.IsFull ? new DynamicJsonArray() : new DynamicJsonArray { backupHistoryEntryToAdd.ToJson() };

                            var backupHistoryJsonValue = new DynamicJsonValue
                            {
                                [nameof(BackupHistory)] = new DynamicJsonArray
                                {
                                    new DynamicJsonValue
                                    {
                                        [nameof(BackupHistory.FullBackup)] = fullBackup, 
                                        [nameof(BackupHistory.IncrementalBackups)] = increments
                                    }
                                }
                            };

                            blittable = context.ReadObject(backupHistoryJsonValue, itemKey);
                        }
                        else
                        {
                            // We are already have history entries. We'll add a new value according to the given structure.
                            var ptr = tvr.Read(2, out int size);
                            blittable = new BlittableJsonReaderObject(ptr, size, context);

                            blittable.TryGet(nameof(BackupHistory), out BlittableJsonReaderArray backupHistoryItems);

                            if (backupHistoryEntryToAdd.IsFull)
                            {
                                // It's a full backup. Let's add a new backup history entry if it's unique.
                                var isUnique = true;
                                foreach (var entry in backupHistoryItems)
                                {
                                    ((BlittableJsonReaderObject)entry).TryGet(nameof(BackupHistory.FullBackup), out BlittableJsonReaderObject fullBackup);
                                    fullBackup.TryGet(nameof(BackupHistoryEntry.CreatedAt), out DateTime entryCreatedAt);

                                    if (entryCreatedAt != backupHistoryEntryToAdd.CreatedAt)
                                        continue;

                                    isUnique = false;
                                    break;
                                }

                                if (isUnique)
                                {
                                    backupHistoryItems.Modifications ??= new DynamicJsonArray();
                                    backupHistoryItems.Modifications.Add(new DynamicJsonValue
                                    {
                                        [nameof(BackupHistory.FullBackup)] = backupHistoryEntryToAdd.ToJson(),
                                        [nameof(BackupHistory.IncrementalBackups)] = new DynamicJsonArray()
                                    });
                                }
                            }
                            else
                            {
                                // It's an incremental backup.
                                for (int i = backupHistoryItems.Length - 1; i >= 0; i--)
                                {
                                    // We'll find (starting from the end) which full backup we are incrementing.
                                    var entry = (BlittableJsonReaderObject)backupHistoryItems[i];

                                    if (entry.TryGet(nameof(BackupHistory.FullBackup), out BlittableJsonReaderObject fullBackup) == false ||
                                        fullBackup.TryGet(nameof(BackupHistoryEntry.LastFullBackup), out DateTime lastFullBackup) == false ||
                                        backupHistoryEntryToAdd.LastFullBackup != lastFullBackup)
                                        continue;

                                    // We found which full backup was incremented; now we'll append new entry.
                                    entry.TryGet(nameof(BackupHistory.IncrementalBackups), out BlittableJsonReaderArray increments);
                                    increments.Modifications ??= new DynamicJsonArray();
                                    if (increments.Length == 0)
                                    {
                                        increments.Modifications.Add(backupHistoryEntryToAdd.ToJson());
                                        break;
                                    }

                                    var lastIncremental = (BlittableJsonReaderObject)increments[^1];
                                    lastIncremental.TryGet(nameof(BackupHistoryEntry.CreatedAt), out DateTime lastIncrementalCreatedAt);

                                    if (lastIncrementalCreatedAt < backupHistoryEntryToAdd.CreatedAt)
                                        increments.Modifications.Add(backupHistoryEntryToAdd.ToJson());
                                }
                            }

                            var numberOfItems = backupHistoryItems.Length + (backupHistoryItems.Modifications?.Items.Count ?? 0);
                            if (numberOfItems > MaxNumberOfFullBackupsInBackupHistory)
                            {
                                var numberToRemove = numberOfItems - MaxNumberOfFullBackupsInBackupHistory;

                                backupHistoryItems.Modifications ??= new DynamicJsonArray();
                                backupHistoryItems.Modifications.Removals ??= new List<int>();
                                _backupDetailsIdsToDelete ??= new List<string>();
                                for (int i = 0; i < numberToRemove; i++)
                                {
                                    backupHistoryItems.Modifications.Removals.Add(i);

                                    var itemToDelete = (BlittableJsonReaderObject)backupHistoryItems[i];

                                    itemToDelete.TryGet(nameof(BackupHistory.FullBackup), out BlittableJsonReaderObject fullBackup);

                                    fullBackup.TryGet(nameof(BackupHistoryEntry.TaskId), out long taskId);
                                    fullBackup.TryGet(nameof(BackupHistoryEntry.NodeTag), out string nodeTag);
                                    fullBackup.TryGet(nameof(BackupHistoryEntry.CreatedAt), out DateTime fullBackupCreatedAt);
                                    _backupDetailsIdsToDelete.Add(BackupHistoryTableValue.GenerateKey(DatabaseName, taskId, nodeTag, fullBackupCreatedAt,
                                        BackupHistoryItemType.Details));

                                    itemToDelete.TryGet(nameof(BackupHistory.IncrementalBackups), out BlittableJsonReaderArray increments);
                                    foreach (var increment in increments)
                                    {
                                        ((BlittableJsonReaderObject)increment).TryGet(nameof(BackupHistoryEntry.CreatedAt), out DateTime incrementCreatedAt);

                                        _backupDetailsIdsToDelete.Add(BackupHistoryTableValue.GenerateKey(DatabaseName, taskId, nodeTag, incrementCreatedAt,
                                            BackupHistoryItemType.Details));
                                    }
                                }
                            }

                            blittable = context.ReadObject(blittable, itemKey);
                        }

                        ClusterStateMachine.UpdateValue(index, items, valueNameLowered, valueName, blittable);
                    }
                });
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(PeriodicBackupStatus)] = PeriodicBackupStatus.ToJson();
            json[nameof(BackupHistoryEntries)] = new DynamicJsonArray(BackupHistoryEntries);
            json[nameof(MaxNumberOfFullBackupsInBackupHistory)] = MaxNumberOfFullBackupsInBackupHistory;
        }
    }
}
