using System;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.ServerWide.Context;
using Xunit;

namespace FastTests
{
    public abstract partial class RavenTestBase
    {
        public class BackupTestBase
        {
            internal static readonly Lazy<BackupTestBase> Instance = new Lazy<BackupTestBase>(() => new BackupTestBase());

            /// <summary>
            /// Run backup with provided task id and wait for completion. Full backup by default.
            /// </summary>
            public void RunBackup(RavenServer server, long taskId, DocumentStore store, bool isFullBackup = true, OperationStatus opStatus = OperationStatus.Completed, int timeout = 15000)
            {
                AsyncHelpers.RunSync(() => RunBackupAsync(server, taskId, store, isFullBackup, opStatus, timeout));
            }

            /// <summary>
            /// Run backup with provided task id and wait for completion. Full backup by default.
            /// </summary>
            public async Task RunBackupAsync(RavenServer server, long taskId, DocumentStore store, bool isFullBackup = true, OperationStatus opStatus = OperationStatus.Completed, int timeout = 15000)
            {
                var documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                var periodicBackupRunner = documentDatabase.PeriodicBackupRunner;
                var op = periodicBackupRunner.StartBackupTask(taskId, isFullBackup);
                var value = await WaitForValueAsync(async () =>
                {
                    var status = (await store.Maintenance.SendAsync(new GetOperationStateOperation(op))).Status;
                    return status;
                }, opStatus, timeout: timeout);

                Assert.Equal(opStatus, value);
            }

            /// <summary>
            /// Update backup config, run backup and wait for completion. Full backup by default.
            /// </summary>
            /// <returns>TaskId</returns>
            public long UpdateConfigAndRunBackup(RavenServer server, PeriodicBackupConfiguration config, DocumentStore store, bool isFullBackup = true, OperationStatus opStatus = OperationStatus.Completed)
            {
                return AsyncHelpers.RunSync(() => UpdateConfigAndRunBackupAsync(server, config, store, isFullBackup, opStatus));
            }

            /// <summary>
            /// Update backup config, run backup and wait for completion. Full backup by default.
            /// </summary>
            /// <returns>TaskId</returns>
            public async Task<long> UpdateConfigAndRunBackupAsync(RavenServer server, PeriodicBackupConfiguration config, DocumentStore store, bool isFullBackup = true, OperationStatus opStatus = OperationStatus.Completed)
            {
                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                await RunBackupAsync(server, result.TaskId, store, isFullBackup, opStatus);
                return result.TaskId;
            }

            /// <summary>
            /// Run backup with provided task id and wait for completion. Full backup by default.
            /// </summary>
            /// <returns>PeriodicBackupStatus</returns>
            public PeriodicBackupStatus RunBackupAndReturnStatus(RavenServer server, long taskId, DocumentStore store, bool isFullBackup = true, OperationStatus opStatus = OperationStatus.Completed, long? expectedEtag = default, int timeout = 15000)
            {
                return AsyncHelpers.RunSync(() => RunBackupAndReturnStatusAsync(server, taskId, store, isFullBackup, opStatus, expectedEtag, timeout));
            }

            /// <summary>
            /// Run backup with provided task id and wait for completion. Full backup by default.
            /// </summary>
            /// <returns>PeriodicBackupStatus</returns>
            public async Task<PeriodicBackupStatus> RunBackupAndReturnStatusAsync(RavenServer server, long taskId, DocumentStore store, bool isFullBackup = true, OperationStatus opStatus = OperationStatus.Completed, long? expectedEtag = default, int timeout = 15000)
            {
                await RunBackupAsync(server, taskId, store, isFullBackup, opStatus, timeout);
                var operation = new GetPeriodicBackupStatusOperation(taskId);

                PeriodicBackupStatus status = null;
                if (expectedEtag.HasValue)
                {
                    var etag = await WaitForValueAsync(async () =>
                    {
                        status = (await store.Maintenance.SendAsync(operation)).Status;
                        return status.LastEtag;
                    }, expectedEtag.Value, interval: 333);
                    Assert.NotNull(etag);
                    Assert.Equal(expectedEtag.Value, etag.Value);
                }
                else
                {
                    status = (await store.Maintenance.SendAsync(operation)).Status;
                }

                return status;
            }

            public PeriodicBackupConfiguration CreateBackupConfiguration(string backupPath = null, BackupType backupType = BackupType.Backup, bool disabled = false, string fullBackupFrequency = "0 0 1 1 *",
                string incrementalBackupFrequency = null, long? taskId = null, string mentorNode = null, BackupEncryptionSettings backupEncryptionSettings = null, AzureSettings azureSettings = null,
                GoogleCloudSettings googleCloudSettings = null, S3Settings s3Settings = null, RetentionPolicy retentionPolicy = null, string name = null)
            {
                var config = new PeriodicBackupConfiguration()
                {
                    BackupType = backupType,
                    FullBackupFrequency = fullBackupFrequency,
                    Disabled = disabled
                };

                if (taskId.HasValue)
                    config.TaskId = taskId.Value;
                if (string.IsNullOrEmpty(mentorNode) == false)
                    config.MentorNode = mentorNode;
                if (string.IsNullOrEmpty(name) == false)
                    config.Name = name;
                if (string.IsNullOrEmpty(incrementalBackupFrequency) == false)
                    config.IncrementalBackupFrequency = incrementalBackupFrequency;
                if (string.IsNullOrEmpty(backupPath) == false)
                    config.LocalSettings = new LocalSettings { FolderPath = backupPath };
                if (backupEncryptionSettings != null)
                    config.BackupEncryptionSettings = backupEncryptionSettings;
                if (azureSettings != null)
                    config.AzureSettings = azureSettings;
                if (googleCloudSettings != null)
                    config.GoogleCloudSettings = googleCloudSettings;
                if (s3Settings != null)
                    config.S3Settings = s3Settings;
                if (retentionPolicy != null)
                    config.RetentionPolicy = retentionPolicy;

                return config;
            }

            public async Task<string> GetBackupResponsibleNode(RavenServer server, long taskId, string databaseName, bool keepTaskOnOriginalMemberNode = false)
            {
                using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var db = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName).ConfigureAwait(false);
                    var rawRecord = server.ServerStore.Cluster.ReadRawDatabaseRecord(context, databaseName);
                    var pbConfig = rawRecord.GetPeriodicBackupConfiguration(taskId);
                    var backupStatus = db.PeriodicBackupRunner.GetBackupStatus(taskId);
                    var node = db.WhoseTaskIsIt(rawRecord.Topology, pbConfig, backupStatus, keepTaskOnOriginalMemberNode);

                    return node;
                }
            }

            /// <summary>
            /// Create and run backup with provided task id in cluster.
            /// </summary>
            /// <returns>TaskId</returns>
            public long CreateAndRunBackupInCluster(PeriodicBackupConfiguration config, DocumentStore store, bool isFullBackup = true, OperationStatus opStatus = OperationStatus.Completed)
            {
                return AsyncHelpers.RunSync(() => CreateAndRunBackupInClusterAsync(config, store, isFullBackup, opStatus));
            }

            /// <summary>
            /// Create and run backup with provided task id in cluster.
            /// </summary>
            /// <returns>TaskId</returns>
            public async Task<long> CreateAndRunBackupInClusterAsync(PeriodicBackupConfiguration config, DocumentStore store, bool isFullBackup = true, OperationStatus opStatus = OperationStatus.Completed)
            {
                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                await RunBackupInClusterAsync(store, backupTaskId, isFullBackup, opStatus);
                return backupTaskId;
            }

            /// <summary>
            /// Run backup with provided task id in a cluster.
            /// </summary>
            public void RunBackupInCluster(DocumentStore store, long taskId, bool isFullBackup = true, OperationStatus opStatus = OperationStatus.Completed)
            {
                AsyncHelpers.RunSync(() => RunBackupInClusterAsync(store, taskId, isFullBackup, opStatus));
            }

            /// <summary>
            /// Run backup with provided task id in a cluster.
            /// </summary>
            public async Task RunBackupInClusterAsync(DocumentStore store, long taskId, bool isFullBackup = true, OperationStatus opStatus = OperationStatus.Completed)
            {
                var op = await store.Maintenance.SendAsync(new StartBackupOperation(isFullBackup, taskId));

                var value = await WaitForValueAsync(async () =>
                {
                    var x = await store.Maintenance.SendAsync(new GetOperationStateOperation(op.Result.OperationId, op.Result.ResponsibleNode));
                    if (x == null)
                        return OperationStatus.Canceled;

                    OperationStatus status = x.Status;
                    return status;
                }, opStatus);

                Assert.Equal(opStatus, value);
            }

            public IDisposable RestoreDatabase(IDocumentStore store, RestoreBackupConfiguration config, TimeSpan? timeout = null)
            {
                var restoreOperation = new RestoreBackupOperation(config);

                var operation = store.Maintenance.Server.Send(restoreOperation);
                operation.WaitForCompletion(timeout ?? TimeSpan.FromSeconds(30));

                return EnsureDatabaseDeletion(config.DatabaseName, store);
            }

            public IDisposable RestoreDatabaseFromCloud(IDocumentStore store, RestoreBackupConfigurationBase config, TimeSpan? timeout = null)
            {
                var restoreOperation = new RestoreBackupOperation(config);

                var operation = store.Maintenance.Server.Send(restoreOperation);
                operation.WaitForCompletion(timeout ?? TimeSpan.FromSeconds(30));

                return EnsureDatabaseDeletion(config.DatabaseName, store);
            }

            public long GetBackupOperationId(IDocumentStore store, long taskId) => AsyncHelpers.RunSync(() => GetBackupOperationIdAsync(store, taskId));

            public async Task<long> GetBackupOperationIdAsync(IDocumentStore store, long taskId)
            {
                var operation = new GetPeriodicBackupStatusOperation(taskId);
                var result = await store.Maintenance.SendAsync(operation);
                Assert.NotNull(result);
                Assert.NotNull(result.Status);
                Assert.NotNull(result.Status.LastOperationId);
                return result.Status.LastOperationId.Value;
            }

            internal static string PrintBackupStatus(PeriodicBackupStatus status)
            {
                var sb = new StringBuilder();
                if (status == null)
                    return $"{nameof(PeriodicBackupStatus)} is null";

                var isFull = status.IsFull ? "a full" : "an incremental";
                sb.AppendLine($"{nameof(PeriodicBackupStatus)} of backup task '{status.TaskId}', executed {isFull} '{status.BackupType}' on node '{status.NodeTag}' in '{status.DurationInMs}' ms.");
                sb.AppendLine("Debug Info: ");
                sb.AppendLine($"{nameof(PeriodicBackupStatus.LastDatabaseChangeVector)}: '{status.LastDatabaseChangeVector}'");
                sb.AppendLine($"{nameof(PeriodicBackupStatus.LastEtag)}: {status.LastEtag}'");
                sb.AppendLine($"{nameof(PeriodicBackupStatus.LastOperationId)}: '{status.LastOperationId}'");
                sb.AppendLine($"{nameof(PeriodicBackupStatus.LastRaftIndex)}: '{status.LastRaftIndex}'");
                sb.AppendLine($"{nameof(PeriodicBackupStatus.LastFullBackupInternal)}: '{status.LastFullBackupInternal}'");
                sb.AppendLine($"{nameof(PeriodicBackupStatus.LastIncrementalBackupInternal)}: '{status.LastIncrementalBackupInternal}'");
                sb.AppendLine($"{nameof(PeriodicBackupStatus.LastFullBackup)}: '{status.LastFullBackup}'");
                sb.AppendLine($"{nameof(PeriodicBackupStatus.LastIncrementalBackup)}: '{status.LastIncrementalBackup}'");
                sb.AppendLine();

                if (status.Error == null && string.IsNullOrEmpty(status.LocalBackup?.Exception))
                {
                    sb.AppendLine("There were no errors.");
                }
                else
                {
                    sb.AppendLine("There were the following errors during backup task execution:");
                    if (status.Error != null)
                    {
                        sb.AppendLine($"{nameof(PeriodicBackupStatus)}.{nameof(PeriodicBackupStatus.Error)}: ");
                        sb.AppendLine(status.Error.Exception);
                    }

                    if (string.IsNullOrEmpty(status.LocalBackup?.Exception) == false)
                    {
                        sb.AppendLine($"{nameof(PeriodicBackupStatus)}.{nameof(PeriodicBackupStatus.LocalBackup)}.{nameof(PeriodicBackupStatus.LocalBackup.Exception)}: ");
                        sb.AppendLine(status.LocalBackup?.Exception);
                    }
                }

                sb.AppendLine();
                sb.AppendLine("Backup upload status:");

                if (status.UploadToAzure == null)
                {
                    sb.AppendLine($"{nameof(PeriodicBackupStatus.UploadToAzure)} of backup task '{status.TaskId}' is null.");
                }
                else if (status.UploadToAzure.Skipped)
                {
                    sb.AppendLine($"{nameof(PeriodicBackupStatus.UploadToAzure)} of backup task '{status.TaskId}' was skipped.");
                }
                else
                {
                    if (string.IsNullOrEmpty(status.UploadToAzure.Exception) == false)
                    {
                        sb.AppendLine($"{nameof(PeriodicBackupStatus)}.{nameof(PeriodicBackupStatus.UploadToAzure)}.{nameof(PeriodicBackupStatus.UploadToAzure.Exception)}:");
                        sb.AppendLine(status.UploadToAzure?.Exception);
                    }
                    else
                    {
                        sb.AppendLine($"{nameof(PeriodicBackupStatus.UploadToAzure)} of backup task '{status.TaskId}', ran successfully in '{status.UploadToAzure.UploadProgress.UploadTimeInMs}' ms, size: '{status.UploadToAzure.UploadProgress.TotalInBytes}' bytes.");
                    }
                }
                if (status.UploadToFtp == null)
                {
                    sb.AppendLine($"{nameof(PeriodicBackupStatus.UploadToFtp)} of backup task '{status.TaskId}' is null.");
                }
                else if (status.UploadToFtp.Skipped)
                {
                    sb.AppendLine($"{nameof(PeriodicBackupStatus.UploadToFtp)} of backup task '{status.TaskId}' was skipped.");
                }
                else
                {
                    if (string.IsNullOrEmpty(status.UploadToFtp.Exception) == false)
                    {
                        sb.AppendLine($"{nameof(PeriodicBackupStatus)}.{nameof(PeriodicBackupStatus.UploadToFtp)}.{nameof(PeriodicBackupStatus.UploadToFtp.Exception)}:");
                        sb.AppendLine(status.UploadToFtp?.Exception);
                    }
                    else
                    {
                        sb.AppendLine($"{nameof(PeriodicBackupStatus.UploadToFtp)} of backup task '{status.TaskId}', ran successfully in '{status.UploadToFtp.UploadProgress.UploadTimeInMs}' ms, size: '{status.UploadToFtp.UploadProgress.TotalInBytes}' bytes.");
                    }
                }
                if (status.UploadToGlacier == null)
                {
                    sb.AppendLine($"{nameof(PeriodicBackupStatus.UploadToGlacier)} of backup task '{status.TaskId}' is null.");
                }
                else if (status.UploadToGlacier.Skipped)
                {
                    sb.AppendLine($"{nameof(PeriodicBackupStatus.UploadToGlacier)} of backup task '{status.TaskId}' was skipped.");
                }
                else
                {
                    if (string.IsNullOrEmpty(status.UploadToGlacier.Exception) == false)
                    {
                        sb.AppendLine($"{nameof(PeriodicBackupStatus)}.{nameof(PeriodicBackupStatus.UploadToGlacier)}.{nameof(PeriodicBackupStatus.UploadToGlacier.Exception)}:");
                        sb.AppendLine(status.UploadToGlacier?.Exception);
                    }
                    else
                    {
                        sb.AppendLine($"{nameof(PeriodicBackupStatus.UploadToGlacier)} of backup task '{status.TaskId}', ran successfully in '{status.UploadToGlacier.UploadProgress.UploadTimeInMs}' ms, size: '{status.UploadToGlacier.UploadProgress.TotalInBytes}' bytes.");
                    }
                }
                if (status.UploadToGoogleCloud == null)
                {
                    sb.AppendLine($"{nameof(PeriodicBackupStatus.UploadToGoogleCloud)} of backup task '{status.TaskId}' is null.");
                }
                else if (status.UploadToGoogleCloud.Skipped)
                {
                    sb.AppendLine($"{nameof(PeriodicBackupStatus.UploadToGoogleCloud)} of backup task '{status.TaskId}' was skipped.");
                }
                else
                {
                    if (string.IsNullOrEmpty(status.UploadToGoogleCloud.Exception) == false)
                    {
                        sb.AppendLine($"{nameof(PeriodicBackupStatus)}.{nameof(PeriodicBackupStatus.UploadToGoogleCloud)}.{nameof(PeriodicBackupStatus.UploadToGoogleCloud.Exception)}:");
                        sb.AppendLine(status.UploadToGoogleCloud?.Exception);
                    }
                    else
                    {
                        sb.AppendLine($"{nameof(PeriodicBackupStatus.UploadToGoogleCloud)} of backup task '{status.TaskId}', ran successfully in '{status.UploadToGoogleCloud.UploadProgress.UploadTimeInMs}' ms, size: '{status.UploadToGoogleCloud.UploadProgress.TotalInBytes}' bytes.");
                    }
                }
                if (status.UploadToS3 == null)
                {
                    sb.AppendLine($"{nameof(PeriodicBackupStatus.UploadToS3)} of backup task '{status.TaskId}' is null.");
                }
                else if (status.UploadToS3.Skipped)
                {
                    sb.AppendLine($"{nameof(PeriodicBackupStatus.UploadToS3)} of backup task '{status.TaskId}' was skipped.");
                }
                else
                {
                    if (string.IsNullOrEmpty(status.UploadToS3.Exception) == false)
                    {
                        sb.AppendLine($"{nameof(PeriodicBackupStatus)}.{nameof(PeriodicBackupStatus.UploadToS3)}.{nameof(PeriodicBackupStatus.UploadToS3.Exception)}:");
                        sb.AppendLine(status.UploadToS3?.Exception);
                    }
                    else
                    {
                        sb.AppendLine($"{nameof(PeriodicBackupStatus.UploadToS3)} of backup task '{status.TaskId}', ran successfully in '{status.UploadToS3.UploadProgress.UploadTimeInMs}' ms, size: '{status.UploadToS3.UploadProgress.TotalInBytes}' bytes.");
                    }
                }

                return sb.ToString();
            }
        }
    }
}
