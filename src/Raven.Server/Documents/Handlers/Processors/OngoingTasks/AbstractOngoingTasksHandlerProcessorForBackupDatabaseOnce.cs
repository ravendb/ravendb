using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Json;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal abstract class AbstractOngoingTasksHandlerProcessorForBackupDatabaseOnce<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractOngoingTasksHandlerProcessorForBackupDatabaseOnce([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        private static int _oneTimeBackupCounter;

        protected abstract void ScheduleBackup(BackupConfiguration backupConfiguration, long operationId, string backupName, Stopwatch sw, OperationCancelToken token);

        protected abstract long GetNextOperationId();

        protected abstract AbstractNotificationCenter GetNotificationCenter();

        protected virtual void AssertBackup(BackupConfiguration configuration)
        {
            BackupConfigurationHelper.AssertOneTimeBackup(configuration, ServerStore);
        }

        public override async ValueTask ExecuteAsync()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "database-backup");
                var backupConfiguration = JsonDeserializationServer.BackupConfiguration(json);
                var backupName = $"One Time Backup #{Interlocked.Increment(ref _oneTimeBackupCounter)}";
                var operationId = RequestHandler.GetLongQueryString("operationId", required: false) ?? GetNextOperationId();

                BackupUtils.CheckServerHealthBeforeBackup(ServerStore, backupName);

                AssertBackup(backupConfiguration);

                var sw = Stopwatch.StartNew();
                ServerStore.ConcurrentBackupsCounter.StartBackup(backupName, Logger);
                try
                {
                    var cancelToken = RequestHandler.CreateOperationToken();

                    ScheduleBackup(backupConfiguration, operationId, backupName, sw, cancelToken);

                    await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                    {
                        writer.WriteOperationIdAndNodeTag(context, operationId, ServerStore.NodeTag);
                    }
                }
                catch (Exception e)
                {
                    ServerStore.ConcurrentBackupsCounter.FinishBackup(backupName, backupStatus: null, sw.Elapsed, Logger);

                    var message = $"Failed to run backup: '{backupName}'";

                    if (Logger.IsOperationsEnabled)
                        Logger.Operations(message, e);

                    GetNotificationCenter().Add(AlertRaised.Create(
                        RequestHandler.DatabaseName,
                        message,
                        null,
                        AlertType.PeriodicBackup,
                        NotificationSeverity.Error,
                        details: new ExceptionDetails(e)));

                    throw;
                }
            }
        }
    }
}
