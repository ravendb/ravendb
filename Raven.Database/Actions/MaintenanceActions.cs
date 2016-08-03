// -----------------------------------------------------------------------
//  <copyright file="MaintenanceActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Database.Config;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Util;
using Raven.Json.Linq;

using Voron.Impl.Backup;

namespace Raven.Database.Actions
{
    public class MaintenanceActions : ActionsBase
    {
        private readonly ConcurrentQueue<DeleteIndexTask> pendingDeletions = new ConcurrentQueue<DeleteIndexTask>();
        private int lockStatus = 0;
        private const int Locked = 1;
        private const int UnLocked = 0;

        public MaintenanceActions(DocumentDatabase database, SizeLimitedConcurrentDictionary<string, TouchedDocumentInfo> recentTouches, IUuidGenerator uuidGenerator, ILog log)
            : base(database, recentTouches, uuidGenerator, log)
        {
        }

        internal static string FindDatabaseDocument(string rootBackupPath)
        {
            // try to find newest database document in incremental backups first - to have the most recent version (if available)

            var backupPath = Directory.GetDirectories(rootBackupPath, "Inc*")
                                       .OrderByDescending(dir => dir)
                                       .Select(dir => Path.Combine(dir, Constants.DatabaseDocumentFilename))
                                       .FirstOrDefault();

            return backupPath ?? Path.Combine(rootBackupPath, Constants.DatabaseDocumentFilename);
        }

        public static void Restore(RavenConfiguration configuration, DatabaseRestoreRequest restoreRequest, Action<string> output)
        {
            var databaseDocumentPath = FindDatabaseDocument(restoreRequest.BackupLocation);
            if (File.Exists(databaseDocumentPath) == false)
            {
                throw new InvalidOperationException("Cannot restore when the Database.Document file is missing in the backup folder: " + restoreRequest.BackupLocation);
            }

            if (File.Exists(Path.Combine(restoreRequest.BackupLocation, Constants.BackupFailureMarker)))
            {
                throw new InvalidOperationException("Backup failure marker was detected. Unable to restore from given directory.");
            }

            var databaseDocumentText = File.ReadAllText(databaseDocumentPath);
            var databaseDocument = RavenJObject.Parse(databaseDocumentText).JsonDeserialization<DatabaseDocument>();

            string storage;
            if (databaseDocument.Settings.TryGetValue("Raven/StorageTypeName", out storage) == false)
            {
                if (File.Exists(Path.Combine(restoreRequest.BackupLocation, BackupMethods.Filename))) 
                    storage = InMemoryRavenConfiguration.VoronTypeName;
                else if (Directory.Exists(Path.Combine(restoreRequest.BackupLocation, "new")))
                    storage = InMemoryRavenConfiguration.EsentTypeName;
                else
                    storage = InMemoryRavenConfiguration.EsentTypeName;
            }

            if (!string.IsNullOrWhiteSpace(restoreRequest.DatabaseLocation))
            {
                configuration.DataDirectory = restoreRequest.DatabaseLocation;
            }

            using (var transactionalStorage = configuration.CreateTransactionalStorage(storage, () => { }, () => { }))
            {
                transactionalStorage.Restore(restoreRequest, output);
            }
        }

        public Task StartBackup(string backupDestinationDirectory, bool incrementalBackup, DatabaseDocument databaseDocument, ResourceBackupState state, CancellationToken token = default(CancellationToken))
        {
            if (databaseDocument == null) throw new ArgumentNullException("databaseDocument");
            var document = Database.Documents.Get(BackupStatus.RavenBackupStatusDocumentKey, null);
            if (document != null)
            {
                var backupStatus = document.DataAsJson.JsonDeserialization<BackupStatus>();
                if (backupStatus.IsRunning)
                {
                    throw new InvalidOperationException("Backup is already running");
                }
            }

            if (File.Exists(Path.Combine(backupDestinationDirectory, Constants.BackupFailureMarker)))
            {
                throw new InvalidOperationException("Backup failure marker was detected. In order to proceed remove old backup files or supply different backup directory.");
            }

            bool enableIncrementalBackup;
            if (incrementalBackup &&
                TransactionalStorage is Raven.Storage.Esent.TransactionalStorage &&
                (bool.TryParse(Database.Configuration.Settings[Constants.Esent.CircularLog], out enableIncrementalBackup) == false || enableIncrementalBackup))
            {
                throw new InvalidOperationException("In order to run incremental backups using Esent you must have circular logging disabled");
            }

            if (incrementalBackup &&
                TransactionalStorage is Raven.Storage.Voron.TransactionalStorage &&
                Database.Configuration.Storage.Voron.AllowIncrementalBackups == false)
            {
                throw new InvalidOperationException("In order to run incremental backups using Voron you must have the appropriate setting key (Raven/Voron/AllowIncrementalBackups) set to true");
            }

            Database.Documents.Put(BackupStatus.RavenBackupStatusDocumentKey, null, RavenJObject.FromObject(new BackupStatus
            {
                Started = SystemTime.UtcNow,
                IsRunning = true,
            }), new RavenJObject(), null);

            Database.IndexStorage.FlushMapIndexes();
            Database.IndexStorage.FlushReduceIndexes();

            if (databaseDocument.Settings.ContainsKey("Raven/StorageTypeName") == false)
                databaseDocument.Settings["Raven/StorageTypeName"] = TransactionalStorage.FriendlyName ?? TransactionalStorage.GetType().AssemblyQualifiedName;

            var linkedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(token, Database.WorkContext.CancellationToken);
            return TransactionalStorage.StartBackupOperation(Database, backupDestinationDirectory, incrementalBackup, databaseDocument, state, linkedTokenSource.Token)
                .ContinueWith(_ => linkedTokenSource.Dispose());
        }

        public void RemoveAllBefore(string listName,Etag etag)
        {
            TransactionalStorage.Batch(accessor => accessor.Lists.RemoveAllBefore(listName,etag));
        }

        public void PurgeOutdatedTombstones()
        {
            var tomstoneLists = new[]
            {
                Constants.RavenPeriodicExportsAttachmentsTombstones,
                Constants.RavenPeriodicExportsDocsTombstones,
                Constants.RavenReplicationAttachmentsTombstones,
                Constants.RavenReplicationDocsTombstones
            };

            var olderThan = SystemTime.UtcNow.Subtract(Database.Configuration.TombstoneRetentionTime);

            foreach (var listName in tomstoneLists)
            {
                string name = listName;
                TransactionalStorage.Batch(accessor => accessor.Lists.RemoveAllOlderThan(name, olderThan));
            }
        }

        public void DeleteRemovedIndexes(Dictionary<int, DocumentDatabase.IndexFailDetails> reason)
        {
            var pendingDeletionData = new List<RavenJObject>();
            var idsOfLostIndexes = new List<int>();

            TransactionalStorage.Batch(actions =>
            {
                foreach (var result in actions.Lists.Read("Raven/Indexes/PendingDeletion", Etag.Empty, null, 100))
                {
                    pendingDeletionData.Add(result.Data);
                }

                var indexIds = actions.Indexing.GetIndexesStats().Select(x => x.Id).ToList();
                foreach (int id in indexIds)
                {
                    var index = IndexDefinitionStorage.GetIndexDefinition(id);
                    if (index != null)
                        continue;

                    idsOfLostIndexes.Add(id);
                }
            });

            foreach (var indexDeletion in pendingDeletionData)
            {
                StartDeletingIndexDataAsync(indexDeletion.Value<int>("IndexId"), indexDeletion.Value<string>("IndexName"));
            }

            Task.Factory.StartNew(() =>
            {
                foreach (var indexId in idsOfLostIndexes)
                {
                    try
                    {
                        // index is not found on disk, better kill for good
                        // Even though technically we are running into a situation that is considered to be corrupt data
                        // we can safely recover from it by removing the other parts of the index.

                        Database.TransactionalStorage.Batch(actions =>
                        {
                            // index is not found on disk, better kill for good
                            // Even though technically we are running into a situation that is considered to be corrupt data
                            // we can safely recover from it by removing the other parts of the index.
                            Database.IndexStorage.DeleteIndex(indexId);
                            actions.Indexing.DeleteIndex(indexId, WorkContext.CancellationToken);

                            string indexName;
                            string msg;
                            string ex;

                            DocumentDatabase.IndexFailDetails failDetails;
                            if (reason == null || reason.TryGetValue(indexId, out failDetails) == false)
                            {
                                indexName = "Unknown Name";
                                msg = string.Format("Index '{0}-({1})' couldn't be found or invalid", indexId, indexName);
                                ex = "";
                            }
                            else
                            {
                                indexName = failDetails.IndexName;
                                msg = failDetails.Reason;
                                ex = failDetails.Ex.ToString();
                            }

                            var logTitle = string.Format("Index '{0}-({1})' removed because it is not found or invalid", indexId, indexName);

                            Log.Error(logTitle);

                            Database.AddAlert(new Alert
                            {
                                AlertLevel = AlertLevel.Error,
                                CreatedAt = SystemTime.UtcNow,
                                Message = msg,
                                Title = logTitle,
                                Exception = ex,
                                UniqueKey = msg
                            });
                        });
                    }
                    catch (Exception e)
                    {
                        Log.ErrorException("Could not delete data from the storage of an index which was not found on the disk. Index id: " + indexId, e);
                    }
                }
            }, TaskCreationOptions.LongRunning);
        }

        public void StartDeletingIndexDataAsync(int indexId, string indexName)
        {
            pendingDeletions.Enqueue(new DeleteIndexTask
            {
                IndexId = indexId,
                IndexName = indexName
            });

            TryCreateIndexesDeletionTask();
        }

        private void TryCreateIndexesDeletionTask()
        {
            if (Interlocked.CompareExchange(ref lockStatus, Locked, UnLocked) == Locked)
                return;

            if (pendingDeletions.IsEmpty)
            {
                Interlocked.Exchange(ref lockStatus, UnLocked);
                return;
            }

            var status = new OperationStateBase();
            var deleteIndexesTask = Task.Factory.StartNew(() =>
            {
                try
                {
                    var success = new List<string>();
                    var failed = new List<string>();
                    var sp = Stopwatch.StartNew();

                    while (true)
                    {
                        DeleteIndexTask pendingDeletion;
                        if (pendingDeletions.TryDequeue(out pendingDeletion) == false)
                            break;

                        var result = DeleteIndex(pendingDeletion, status.MarkProgress);
                        if (result)
                            success.Add(pendingDeletion.IndexName);
                        else
                            failed.Add(pendingDeletion.IndexName);
                    }

                    var message = $"Finished the deletion of {success.Count + failed.Count} indexes in {sp.Elapsed}";
                    if (success.Count > 0)
                        message += $", {success.Count} successful ({string.Join(", ", success)})";
                    if (failed.Count > 0)
                        message += $", {failed.Count} failed ({string.Join(", ", success)})";
                    status.MarkCompleted(message);
                }
                finally
                {
                    Interlocked.Exchange(ref lockStatus, UnLocked);

                    if (pendingDeletions.IsEmpty == false)
                    {
                        //handling a race condition where we get a new deletion
                        //and exiting while thinking that the deletion will be handled
                        TryCreateIndexesDeletionTask();
                    }
                }
            }, TaskCreationOptions.LongRunning);

            long taskId;
            Database.Tasks.AddTask(deleteIndexesTask, status, new TaskActions.PendingTaskDescription
            {
                StartTime = SystemTime.UtcNow,
                TaskType = TaskActions.PendingTaskType.IndexDeleteOperation,
                Description = "Deleting indexes"
            }, out taskId);
        }

        private class DeleteIndexTask
        {
            public int IndexId { get; set; }

            public string IndexName { get; set; }
        }

        private bool DeleteIndex(DeleteIndexTask pendingDeletion, Action<string> markProgress)
        {
            try
            {
                var sp = Stopwatch.StartNew();
                var message = $"Starting deletion of index {pendingDeletion.IndexName}";
                markProgress(message);
                Log.Info(message);

                var indexId = pendingDeletion.IndexId;
                TransactionalStorage.Batch(actions => actions.Indexing.PrepareIndexForDeletion(indexId));

                Debug.Assert(Database.IndexStorage != null);
                Database.IndexStorage.DeleteIndexData(indexId); //data can take a while
                markProgress($"Finished the deletion of index {pendingDeletion.IndexName} " +
                             $"from disk in {sp.Elapsed}, starting the deletion from internal storage");

                TransactionalStorage.Batch(actions =>
                {
                    //and esent data can take a while too
                    actions.Indexing.DeleteIndex(indexId, WorkContext.CancellationToken);
                    if (WorkContext.CancellationToken.IsCancellationRequested)
                        return;

                    actions.Lists.Remove("Raven/Indexes/PendingDeletion", indexId.ToString(CultureInfo.InvariantCulture));
                });

                message = $"The deletion of index {pendingDeletion.IndexName} was completed in {sp.Elapsed}";
                Log.Info(message);
                markProgress(message);
                return true;
            }
            catch (Exception e)
            {
                Log.WarnException("Failure when deleting index " + pendingDeletion.IndexName, e);
                return false;
            }
        }
    }
}
