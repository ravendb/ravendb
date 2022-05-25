using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Util;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Utils;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal class OngoingTasksHandlerProcessorForBackupDatabaseOnce : AbstractOngoingTasksHandlerProcessorForBackupDatabaseOnce<DatabaseRequestHandler, DocumentsOperationContext>
    {
        private static int OneTimeBackupCounter;

        public OngoingTasksHandlerProcessorForBackupDatabaseOnce([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override ValueTask ExecuteBackup(TransactionOperationContext context, BackupConfiguration backupConfiguration, long operationId)
        {
            var backupName = $"One Time Backup #{Interlocked.Increment(ref OneTimeBackupCounter)}";
            AssertCanExecute(backupName, backupConfiguration);

            var sw = Stopwatch.StartNew();
            ServerStore.ConcurrentBackupsCounter.StartBackup(backupName, Logger);
            try
            {
                var cancelToken = RequestHandler.CreateOperationToken();
                var backupParameters = new BackupParameters
                {
                    RetentionPolicy = null,
                    StartTimeUtc = SystemTime.UtcNow,
                    IsOneTimeBackup = true,
                    BackupStatus = new PeriodicBackupStatus { TaskId = -1 },
                    OperationId = operationId,
                    BackupToLocalFolder = BackupConfiguration.CanBackupUsing(backupConfiguration.LocalSettings),
                    IsFullBackup = true,
                    TempBackupPath = (RequestHandler.Database.Configuration.Storage.TempPath ?? RequestHandler.Database.Configuration.Core.DataDirectory).Combine("OneTimeBackupTemp"),
                    Name = backupName
                };

                var backupTask = new BackupTask(RequestHandler.Database, backupParameters, backupConfiguration, Logger);
                var threadName = $"Backup thread {backupName} for database '{RequestHandler.DatabaseName}'";

                var t = RequestHandler.Database.Operations.AddLocalOperation(
                    operationId,
                    OperationType.DatabaseBackup,
                    $"Manual backup for database: {RequestHandler.DatabaseName}",
                    detailedDescription: null,
                    onProgress =>
                    {
                        var tcs = new TaskCompletionSource<IOperationResult>(TaskCreationOptions.RunContinuationsAsynchronously);
                        PoolOfThreads.GlobalRavenThreadPool.LongRunning(x =>
                        {
                            try
                            {
                                ThreadHelper.TrySetThreadPriority(ThreadPriority.BelowNormal, threadName, Logger);
                                NativeMemory.EnsureRegistered();

                                using (RequestHandler.Database.PreventFromUnloadingByIdleOperations())
                                {
                                    var runningBackupStatus = new PeriodicBackupStatus { TaskId = 0, BackupType = backupConfiguration.BackupType };
                                    var backupResult = backupTask.RunPeriodicBackup(onProgress, ref runningBackupStatus);
                                    BackupTask.SaveBackupStatus(runningBackupStatus, RequestHandler.Database, Logger, backupResult);
                                    tcs.SetResult(backupResult);
                                }
                            }
                            catch (OperationCanceledException)
                            {
                                tcs.SetCanceled();
                            }
                            catch (Exception e)
                            {
                                if (Logger.IsOperationsEnabled)
                                    Logger.Operations($"Failed to run the backup thread: '{backupName}'", e);

                                tcs.SetException(e);
                            }
                            finally
                            {
                                ServerStore.ConcurrentBackupsCounter.FinishBackup(backupName, backupStatus: null, sw.Elapsed, Logger);
                            }
                        }, null, threadName);
                        return tcs.Task;
                    },
                    token: cancelToken);

                return ValueTask.CompletedTask;
            }
            catch (Exception e)
            {
                ServerStore.ConcurrentBackupsCounter.FinishBackup(backupName, backupStatus: null, sw.Elapsed, Logger);

                var message = $"Failed to run backup: '{backupName}'";

                if (Logger.IsOperationsEnabled)
                    Logger.Operations(message, e);

                RequestHandler.Database.NotificationCenter.Add(AlertRaised.Create(
                    RequestHandler.DatabaseName,
                    message,
                    null,
                    AlertType.PeriodicBackup,
                    NotificationSeverity.Error,
                    details: new ExceptionDetails(e)));

                throw;
            }
        }

        protected override long GetNextOperationId()
        {
            return ServerStore.Operations.GetNextOperationId();
        }

        private void AssertCanExecute(string backupName, BackupConfiguration backupConfiguration)
        {
            BackupUtils.CheckServerHealthBeforeBackup(ServerStore, backupName);
            ServerStore.LicenseManager.AssertCanAddPeriodicBackup(backupConfiguration);
            BackupConfigurationHelper.AssertBackupConfigurationInternal(backupConfiguration);
            BackupConfigurationHelper.AssertDestinationAndRegionAreAllowed(backupConfiguration, ServerStore);
        }
    }
}
