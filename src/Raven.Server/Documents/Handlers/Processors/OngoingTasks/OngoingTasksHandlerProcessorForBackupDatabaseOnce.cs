using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Extensions;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Server.Utils;
using Sparrow.Utils;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal sealed class OngoingTasksHandlerProcessorForBackupDatabaseOnce : AbstractOngoingTasksHandlerProcessorForBackupDatabaseOnce<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public OngoingTasksHandlerProcessorForBackupDatabaseOnce([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override void ScheduleBackup(BackupConfiguration backupConfiguration, long operationId, string backupName, Stopwatch sw, DateTime startTime, OperationCancelToken token)
        {
            var backupParameters = new BackupParameters
            {
                RetentionPolicy = null,
                StartTimeUtc = startTime,
                IsOneTimeBackup = true,
                BackupStatus = new PeriodicBackupStatus { TaskId = -1 },
                OperationId = operationId,
                BackupToLocalFolder = BackupConfiguration.CanBackupUsing(backupConfiguration.LocalSettings),
                IsFullBackup = true,
                TempBackupPath = BackupUtils.GetBackupTempPath(RequestHandler.Database.Configuration, "OneTimeBackupTemp", out PathSetting _),
                Name = backupName,
            };

            var backupTask = BackupUtils.GetBackupTask(RequestHandler.Database, backupParameters, backupConfiguration, token, Logger, RequestHandler.Database.PeriodicBackupRunner._forTestingPurposes);
            var threadName = $"Backup thread {backupName} for database '{RequestHandler.Database.Name}'";

            var t = RequestHandler.Database.Operations.AddLocalOperation(
                operationId,
                OperationType.DatabaseBackup,
                $"Manual backup for database: {RequestHandler.Database.Name}",
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
                                BackupUtils.SaveBackupStatus(runningBackupStatus, RequestHandler.DatabaseName, RequestHandler.Database.ServerStore, Logger, backupResult);
                                tcs.SetResult(backupResult);
                            }
                        }
                        catch (Exception e) when (e.ExtractSingleInnerException() is OperationCanceledException)
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
                            ServerStore.ConcurrentBackupsCounter.FinishBackup(RequestHandler.DatabaseName, backupName, backupStatus: null, sw.Elapsed, Logger);
                        }
                    }, null, ThreadNames.ForBackup(threadName, backupName, RequestHandler.DatabaseName));
                    return tcs.Task;
                },
                token: token);

            var _ = t.ContinueWith(_ =>
            {
                token.Dispose();
            });
        }

        protected override long GetNextOperationId()
        {
            return RequestHandler.Database.Operations.GetNextOperationId();
        }

        protected override AbstractNotificationCenter GetNotificationCenter()
        {
            return RequestHandler.Database.NotificationCenter;
        }
    }
}
