using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Documents;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Sparrow.Json;
using Xunit;

namespace FastTests
{
    public abstract partial class RavenTestBase
    {
        public readonly BackupTestBase Backup;

        public class BackupTestBase
        {
            private readonly RavenTestBase _parent;
            private readonly int _reasonableTimeout = Debugger.IsAttached ? 60000 : 30000;

            public BackupTestBase(RavenTestBase parent)
            {
                _parent = parent ?? throw new ArgumentNullException(nameof(parent));
            }

            /// <summary>
            /// Run backup with provided task id and wait for completion. Full backup by default.
            /// </summary>
            public void RunBackup(RavenServer server, long taskId, DocumentStore store, bool isFullBackup = true, OperationStatus opStatus = OperationStatus.Completed, int? timeout = default)
            {
                AsyncHelpers.RunSync(() => RunBackupAsync(server, taskId, store, isFullBackup, opStatus, timeout));
            }

            /// <summary>
            /// Run backup with provided task id and wait for completion. Full backup by default.
            /// </summary>
            public async Task<long> RunBackupAsync(RavenServer server, long taskId, IDocumentStore store, bool isFullBackup = true, OperationStatus opStatus = OperationStatus.Completed, int? timeout = default)
            {
                var documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                var periodicBackupRunner = documentDatabase.PeriodicBackupRunner;
                var op = periodicBackupRunner.StartBackupTask(taskId, isFullBackup);

                BackupResult result = default;
                var actual = await WaitForValueAsync(async () =>
                {
                    var state = await store.Maintenance.SendAsync(new GetOperationStateOperation(op));
                    result = state.Result as BackupResult;
                    return state.Status;
                }, opStatus, timeout: timeout ?? _reasonableTimeout);

                await CheckBackupOperationStatus(opStatus, actual, store, taskId, op, periodicBackupRunner, result);
                Assert.Equal(opStatus, actual);
                return op;
            }

            /// <summary>
            /// Update backup config, run backup and wait for completion. Full backup by default.
            /// </summary>
            /// <returns>TaskId</returns>
            public long UpdateConfigAndRunBackup(RavenServer server, PeriodicBackupConfiguration config, DocumentStore store, bool isFullBackup = true, OperationStatus opStatus = OperationStatus.Completed, int? timeout = default)
            {
                return AsyncHelpers.RunSync(() => UpdateConfigAndRunBackupAsync(server, config, store, isFullBackup, opStatus, timeout));
            }

            /// <summary>
            /// Update backup config, run backup and wait for completion. Full backup by default.
            /// </summary>
            /// <returns>TaskId</returns>
            public async Task<long> UpdateConfigAndRunBackupAsync(RavenServer server, PeriodicBackupConfiguration config, IDocumentStore store, bool isFullBackup = true, OperationStatus opStatus = OperationStatus.Completed, int? timeout = default)
            {
                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

                WaitForResponsibleNodeUpdate(server.ServerStore, store.Database, result.TaskId);

                await RunBackupAsync(server, result.TaskId, store, isFullBackup, opStatus, timeout);
                return result.TaskId;
            }

            public async Task<long> UpdateConfigAsync(RavenServer server, PeriodicBackupConfiguration config, DocumentStore store)
            {
                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                
                WaitForResponsibleNodeUpdate(server.ServerStore, store.Database, result.TaskId);

                return result.TaskId;
            }
            
            public async Task<long> UpdateServerWideConfigAsync(RavenServer server, ServerWideBackupConfiguration config, DocumentStore store)
            {
                await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(config));

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var backup = record.PeriodicBackups.First();
                var backupTaskId = backup.TaskId;

                WaitForResponsibleNodeUpdate(server.ServerStore, store.Database, backupTaskId);

                return backupTaskId;
            }

            public void WaitForResponsibleNodeUpdate(ServerStore serverStore, string databaseName, long taskId, string differentThan = null)
            {
                var value = WaitForValue(() =>
                {
                    var responsibleNode = BackupUtils.GetResponsibleNodeTag(serverStore, databaseName, taskId);
                    return responsibleNode != differentThan;
                }, true);

                Assert.True(value);
            }

            /// <summary>
            /// Run backup with provided task id and wait for completion. Full backup by default.
            /// </summary>
            /// <returns>PeriodicBackupStatus</returns>
            public PeriodicBackupStatus RunBackupAndReturnStatus(RavenServer server, long taskId, DocumentStore store, bool isFullBackup = true, OperationStatus opStatus = OperationStatus.Completed, long? expectedEtag = default, int? timeout = default)
            {
                return AsyncHelpers.RunSync(() => RunBackupAndReturnStatusAsync(server, taskId, store, isFullBackup, opStatus, expectedEtag, timeout));
            }

            /// <summary>
            /// Run backup with provided task id and wait for completion. Full backup by default.
            /// </summary>
            /// <returns>PeriodicBackupStatus</returns>
            public async Task<PeriodicBackupStatus> RunBackupAndReturnStatusAsync(RavenServer server, long taskId, DocumentStore store, bool isFullBackup = true, OperationStatus opStatus = OperationStatus.Completed, long? expectedEtag = default, int? timeout = default)
            {
                var opId = await RunBackupAsync(server, taskId, store, isFullBackup, opStatus, timeout);
                var operation = new GetPeriodicBackupStatusOperation(taskId);

                PeriodicBackupStatus status = null;
                if (expectedEtag.HasValue)
                {
                    var etag = await WaitForValueAsync(async () =>
                    {
                        status = (await store.Maintenance.SendAsync(operation)).Status;
                        return status.LastEtag;
                    }, expectedEtag.Value, interval: 333, timeout: timeout ?? _reasonableTimeout);
                    await CheckExceptedEtag(server, store, opId, status, etag, expectedEtag.Value);
                }
                else
                {
                    status = (await store.Maintenance.SendAsync(operation)).Status;
                }

                return status;
            }

            private static async Task CheckExceptedEtag(RavenServer ravenServer, DocumentStore store, long opId, PeriodicBackupStatus status, long? etag, long expectedEtag)
            {
                var backupOperation = store.Maintenance.Send(new GetOperationStateOperation(opId));
                Assert.Equal(OperationStatus.Completed, backupOperation.Status);
                Assert.NotNull(etag);

                if (expectedEtag != etag.Value)
                {
                    var backupResult = backupOperation.Result as BackupResult;
                    var documentDatabase = await ravenServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    var periodicBackupRunner = documentDatabase.PeriodicBackupRunner;
                    TryGetBackupStatusFromPeriodicBackupAndPrint(OperationStatus.Completed, OperationStatus.Completed, opId, periodicBackupRunner, status, backupResult);
                }
            }

            public PeriodicBackupConfiguration CreateBackupConfiguration(string backupPath = null, BackupType backupType = BackupType.Backup, bool disabled = false, string fullBackupFrequency = "0 0 1 1 *",
                string incrementalBackupFrequency = null, long? taskId = null, string mentorNode = null, BackupEncryptionSettings backupEncryptionSettings = null, AzureSettings azureSettings = null,
                GoogleCloudSettings googleCloudSettings = null, S3Settings s3Settings = null, FtpSettings ftpSettings = null, RetentionPolicy retentionPolicy = null, string name = null, BackupUploadMode backupUploadMode = BackupUploadMode.Default, bool pinToMentorNode = false)
            {
                var config = new PeriodicBackupConfiguration()
                {
                    BackupType = backupType,
                    FullBackupFrequency = fullBackupFrequency,
                    Disabled = disabled,
                    BackupUploadMode = backupUploadMode,
                    PinToMentorNode = pinToMentorNode
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
                if (ftpSettings != null)
                    config.FtpSettings = ftpSettings;
                if (retentionPolicy != null)
                    config.RetentionPolicy = retentionPolicy;

                return config;
            }

            public string GetBackupResponsibleNode(RavenServer server, long taskId, string databaseName, bool keepTaskOnOriginalMemberNode = false)
            {
                var node = BackupUtils.GetResponsibleNodeTag(server.ServerStore, databaseName, taskId);
                return node;
            }

            /// <summary>
            /// Create and run backup with provided task id in cluster.
            /// </summary>
            /// <returns>TaskId</returns>
            public long CreateAndRunBackupInCluster(PeriodicBackupConfiguration config, IDocumentStore store, List<RavenServer> nodes, bool isFullBackup = true, OperationStatus opStatus = OperationStatus.Completed, int? timeout = default)
            {
                return AsyncHelpers.RunSync(() => CreateAndRunBackupInClusterAsync(config, store, nodes, isFullBackup, opStatus, timeout));
            }

            /// <summary>
            /// Create and run backup with provided task id in cluster.
            /// </summary>
            /// <returns>TaskId</returns>
            public async Task<long> CreateAndRunBackupInClusterAsync(PeriodicBackupConfiguration config, IDocumentStore store, List<RavenServer> nodes, bool isFullBackup = true, OperationStatus opStatus = OperationStatus.Completed, int? timeout = default)
            {
                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;

                WaitForResponsibleNodeUpdateInCluster(store, nodes, backupTaskId);

                await RunBackupInClusterAsync(store, backupTaskId, isFullBackup, opStatus, timeout);
                return backupTaskId;
            }

            public void WaitForResponsibleNodeUpdateInCluster(IDocumentStore store, List<RavenServer> nodes, long backupTaskId)
            {
                foreach (var server in nodes)
                {
                    WaitForResponsibleNodeUpdate(server.ServerStore, store.Database, backupTaskId);
                }
            }

            /// <summary>
            /// Run backup with provided task id in a cluster.
            /// </summary>
            public void RunBackupInCluster(DocumentStore store, long taskId, bool isFullBackup = true, OperationStatus opStatus = OperationStatus.Completed, int? timeout = default)
            {
                AsyncHelpers.RunSync(() => RunBackupInClusterAsync(store, taskId, isFullBackup, opStatus, timeout));
            }

            /// <summary>
            /// Run backup with provided task id in a cluster.
            /// </summary>
            public async Task RunBackupInClusterAsync(IDocumentStore store, long taskId, bool isFullBackup = true, OperationStatus opStatus = OperationStatus.Completed, int? timeout = default)
            {
                var op = await store.Maintenance.SendAsync(new StartBackupOperation(isFullBackup, taskId));

                IOperationResult result = default;
                var actual = await WaitForValueAsync(async () =>
                {
                    var state = await store.Maintenance.SendAsync(new GetOperationStateOperation(op.Result.OperationId, op.Result.ResponsibleNode));
                    if (state == null)
                        return OperationStatus.Canceled;

                    result = state.Result;
                    return state.Status;
                }, opStatus, timeout: timeout ?? _reasonableTimeout);

                await CheckBackupOperationStatus(opStatus, actual, store, taskId, op.Result.OperationId, periodicBackupRunner: null, result);
                Assert.Equal(opStatus, actual);
            }

            public IDisposable RestoreDatabase(IDocumentStore store, RestoreBackupConfiguration config, TimeSpan? timeout = null, string nodeTag = null)
            {
                RestoreBackupOperation restoreOperation;
                if (nodeTag != null)
                    restoreOperation = new RestoreBackupOperation(config, nodeTag);
                else
                    restoreOperation = new RestoreBackupOperation(config);

                var operation = store.Maintenance.Server.Send(restoreOperation);
                operation.WaitForCompletion(timeout ?? TimeSpan.FromMilliseconds(_reasonableTimeout * 2));

                return _parent.Databases.EnsureDatabaseDeletion(config.DatabaseName, store);
            }

            public IDisposable RestoreDatabaseFromCloud(IDocumentStore store, RestoreBackupConfigurationBase config, TimeSpan? timeout = null)
            {
                var restoreOperation = new RestoreBackupOperation(config);

                var operation = store.Maintenance.Server.Send(restoreOperation);
                operation.WaitForCompletion(timeout ?? TimeSpan.FromMilliseconds(_reasonableTimeout * 2));

                return _parent.Databases.EnsureDatabaseDeletion(config.DatabaseName, store);
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

            public async Task<long> CreateAndRunBackupAsync(IDocumentStore store, RavenDatabaseMode mode, string backupPath)
            {
                var config = CreateBackupConfiguration(backupPath);

                if (mode == RavenDatabaseMode.Single)
                    return await UpdateConfigAndRunBackupAsync(_parent.Server, config, store);

                var backupCompleted = await _parent.Sharding.Backup.WaitForBackupToComplete(store);
                var backupTaskId = await _parent.Sharding.Backup.UpdateConfigurationAndRunBackupAsync(_parent.Server, store, config);
                Assert.True(WaitHandle.WaitAll(backupCompleted, TimeSpan.FromSeconds(10)));

                return backupTaskId;
            }

            public async Task<RestoreBackupConfiguration> RunBackupAndCreateRestoreConfigurationAsync(DocumentStore store, RavenDatabaseMode mode, long backupTaskId, string backupPath)
            {
                var databaseName = _parent.GetDatabaseName();
                var configuration = new RestoreBackupConfiguration { DatabaseName = databaseName };

                if (mode == RavenDatabaseMode.Single)
                {
                    await RunBackupAndReturnStatusAsync(_parent.Server, backupTaskId, store, isFullBackup: false);
                    configuration.BackupLocation = Directory.GetDirectories(backupPath).First();
                }
                else
                {
                    var backupCompleted = await _parent.Sharding.Backup.WaitForBackupToComplete(store);
                    await _parent.Sharding.Backup.RunBackupAsync(store.Database, backupTaskId, isFullBackup: false, servers: new List<RavenServer> { _parent.Server });
                    Assert.True(WaitHandle.WaitAll(backupCompleted, TimeSpan.FromSeconds(10)));

                    var dirs = Directory.GetDirectories(backupPath);
                    var sharding = await _parent.Sharding.GetShardingConfigurationAsync(store);
                    var settings = _parent.Sharding.Backup.GenerateShardRestoreSettings(dirs, sharding);
                    configuration.ShardRestoreSettings = settings;
                }

                return configuration;
            }

            internal static string PrintBackupStatusAndResult(PeriodicBackupStatus status, IOperationResult result)
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
                sb.AppendLine($"{nameof(PeriodicBackupStatus.LastRaftIndex)}: '{status.LastRaftIndex.LastEtag}'");
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

                sb.AppendLine();
                sb.AppendLine("BackupResult properties:");
                if (result != null)
                {
                    using (var context = JsonOperationContext.ShortTermSingleUse())
                    {
                        var djv = result.ToJson();
                        var json = context.ReadObject(djv, "smuggler/result");
                        sb.AppendLine(json.ToString());
                    }
                }
                else
                {
                    sb.AppendLine("No backup result.");
                }

                return sb.ToString();
            }

            private static async Task CheckBackupOperationStatus(OperationStatus expected, OperationStatus actual, IDocumentStore store, long taskId, long opId,
                PeriodicBackupRunner periodicBackupRunner, IOperationResult backupResult)
            {
                if (expected == OperationStatus.Completed && actual == OperationStatus.Faulted)
                {
                    // gather debug info
                    var operation = new GetPeriodicBackupStatusOperation(taskId);
                    var status = (await store.Maintenance.SendAsync(operation)).Status;

                    TryGetBackupStatusFromPeriodicBackupAndPrint(expected, actual, opId, periodicBackupRunner, status, result: null);

                    Assert.Fail($"Backup status expected: '{expected}', actual '{actual}',{Environment.NewLine}" +
                                $"Backup status from storage for current operation id: '{opId}':{Environment.NewLine}" +
                                PrintBackupStatusAndResult(status, backupResult));
                }

                if (expected == OperationStatus.Completed && actual == OperationStatus.InProgress)
                {
                    // backup didn't complete in time, try to print running backup status, and backup result
                    var pb = periodicBackupRunner?.PeriodicBackups.FirstOrDefault(x => x.RunningBackupStatus != null && x.BackupStatus.TaskId == taskId);
                    if (pb == null)
                    {
                        // print previous backup status saved in memory
                        var operation = new GetPeriodicBackupStatusOperation(taskId);
                        var result = await store.Maintenance.SendAsync(operation);
                        Assert.Fail($"Backup status expected: '{expected}', actual '{actual}',{Environment.NewLine}" +
                                    $"Could not fetch running backup status for current task id: '{taskId}', previous backup status:{Environment.NewLine}" +
                                    PrintBackupStatusAndResult(result.Status, backupResult));
                    }

                    Assert.Fail($"Backup status expected: '{expected}', actual '{actual}',{Environment.NewLine}" +
                                $"Running backup status for current task id: '{taskId}':{Environment.NewLine}" +
                                PrintBackupStatusAndResult(pb.RunningBackupStatus, backupResult));
                }
            }

            private static void TryGetBackupStatusFromPeriodicBackupAndPrint(OperationStatus expected, OperationStatus actual, long opId,
                PeriodicBackupRunner periodicBackupRunner, PeriodicBackupStatus status, BackupResult result)
            {
                if (status?.LastOperationId == opId)
                    return;

                // failed to save backup status, lets fetch it from memory
                var pb = periodicBackupRunner?.PeriodicBackups.FirstOrDefault(x => x.BackupStatus != null && x.BackupStatus.LastOperationId == opId);
                if (pb == null)
                {
                    Assert.Fail($"Backup status expected: '{expected}', actual '{actual}',{Environment.NewLine}" +
                                $"Could not fetch backup status for current operation id: '{opId}', previous backup status:{Environment.NewLine}" +
                                PrintBackupStatusAndResult(status, result));
                }

                Assert.Fail($"Backup status expected: '{expected}', actual '{actual}',{Environment.NewLine}" +
                            $"Could not fetch backup status from storage for current operation id: '{opId}', current in memory backup status:{Environment.NewLine}" +
                            PrintBackupStatusAndResult(pb.BackupStatus, result));
            }

            public Task<WaitHandle[]> WaitForBackupToComplete(IDocumentStore store)
            {
                return WaitForBackupsToComplete(new List<IDocumentStore> { store });
            }

            public async Task<WaitHandle[]> WaitForBackupsToComplete(IEnumerable<IDocumentStore> stores)
            {
                var waitHandles = new List<WaitHandle>();
                foreach (var store in stores)
                {
                    var database = await _parent.GetDocumentDatabaseInstanceFor(store);
                    FillBackupCompletionHandles(waitHandles, database);
                }

                return waitHandles.ToArray();
            }

            public static void FillBackupCompletionHandles(List<WaitHandle> waitHandles, DocumentDatabase database)
            {
                var mre = new ManualResetEventSlim();
                waitHandles.Add(mre.WaitHandle);

                database.PeriodicBackupRunner._forTestingPurposes ??= new PeriodicBackupRunner.TestingStuff();
                database.PeriodicBackupRunner._forTestingPurposes.AfterBackupBatchCompleted = () => mre.Set();
            }

            public async Task FillDatabaseWithRandomDataAsync(int databaseSizeInMb, IAsyncDocumentSession session, int? timeout = default)
            {
                var random = new Random();
                const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
                var timeoutTimeSpan = TimeSpan.FromMilliseconds(timeout ?? _reasonableTimeout);

                using (var cts = new CancellationTokenSource(timeoutTimeSpan))
                {
                    for (int i = 0; i < databaseSizeInMb; i++)
                    {
                        var entry = new User
                        {
                            Name = new string(Enumerable.Repeat(chars, 1024 * 1024)
                                .Select(s => s[random.Next(s.Length)]).ToArray())
                        };
                        await session.StoreAsync(entry, cts.Token);
                    }
                    await session.SaveChangesAsync(cts.Token);
                }
            }

            public async Task FillClusterDatabaseWithRandomDataAsync(int databaseSizeInMb, DocumentStore store, int clusterSize, int? timeout = default)
            {
                var timeoutTimeSpan = TimeSpan.FromMilliseconds(timeout ?? _reasonableTimeout);

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(timeoutTimeSpan, replicas: clusterSize - 1);
                    await FillDatabaseWithRandomDataAsync(databaseSizeInMb, session, (int?)timeoutTimeSpan.TotalMilliseconds);
                }   
            }

            internal async Task HoldBackupExecutionIfNeededAndInvoke(PeriodicBackupRunner.TestingStuff ts, Func<Task> func, TaskCompletionSource<object> tcs)
            {
                // hold backup execution 
                try
                {
                    if (ts != null)
                        ts.OnBackupTaskRunHoldBackupExecution = tcs;

                    await func.Invoke();
                }
                finally
                {
                    tcs.TrySetResult(null);
                }
            }
            
            public async Task<string[]> GetBackupFilesAndAssertCountAsync(string backupPath, int expectedCount, long backupOpId, string databaseName, int? shardNumber = null)
            {
                var directory = Directory.GetDirectories(backupPath)
                    .FirstOrDefault(x => shardNumber == null || x.EndsWith($"${shardNumber}"));

                string[] files = null;
                if (directory != null)
                {
                    var filesEnumerable = Directory.GetFiles(directory)
                        .Where(Raven.Client.Documents.Smuggler.BackupUtils.IsBackupFile);
                    files = Raven.Client.Documents.Smuggler.BackupUtils.OrderBackups(filesEnumerable).ToArray();
                }
                
                if (files != null && files.Length != expectedCount)
                {
                    if (shardNumber != null)
                        databaseName += $"${shardNumber}";
                    using var context = JsonOperationContext.ShortTermSingleUse();
                    var database = await _parent.GetDatabase(databaseName);
                    var operation = database.Operations.GetOperation(backupOpId);
                    var jsonOperation = context.ReadObject(operation.ToJson(), "backup operation"); 
                    Assert.Fail($"Expected {expectedCount} backup files but found {files.Length}.\n{string.Join("\n", files)}\n{jsonOperation}");
                }

                return files;
            }

            public async Task<long> RunBackupForDatabaseModeAsync(RavenServer server, PeriodicBackupConfiguration config, IDocumentStore store, RavenDatabaseMode databaseMode, bool isFullBackup = true, long? taskId = null)
            {
                if (taskId == null)
                {
                    taskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;

                    if (databaseMode == RavenDatabaseMode.Sharded)
                        _parent.Sharding.Backup.WaitForResponsibleNodeUpdate(server.ServerStore, store.Database, taskId.Value);
                    else
                        WaitForResponsibleNodeUpdate(server.ServerStore, store.Database, taskId.Value);
                }

                WaitHandle[] waitHandles;
                if (databaseMode == RavenDatabaseMode.Sharded)
                {
                    waitHandles = await _parent.Sharding.Backup.WaitForBackupsToComplete(new[] { store });
                    await _parent.Sharding.Backup.RunBackupAsync(store.Database, taskId.Value, isFullBackup);
                }
                else
                {
                    waitHandles = await WaitForBackupToComplete(store);
                    await RunBackupAsync(server, taskId.Value, store, isFullBackup);
                }

                Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)), "backups failed to complete within 60 seconds");

                return taskId.Value;
            }
        }
    }
}
