using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http.Features.Authentication;
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

        protected abstract void ScheduleBackup(BackupConfiguration backupConfiguration, long operationId, string backupName, Stopwatch sw, DateTime startTime, OperationCancelToken token);

        protected abstract long GetNextOperationId();

        protected abstract AbstractNotificationCenter GetNotificationCenter();

        protected virtual void AssertBackup(BackupConfiguration configuration)
        {
            var authConnection = HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;

            BackupConfigurationHelper.AssertOneTimeBackup(configuration, ServerStore, authConnection);
        }

        public override async ValueTask ExecuteAsync()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "database-backup");
                var backupConfiguration = JsonDeserializationServer.BackupConfiguration(json);
                var backupName = $"One Time Backup #{Interlocked.Increment(ref _oneTimeBackupCounter)}";
                var operationId = RequestHandler.GetLongQueryString("operationId", required: false) ?? GetNextOperationId();
                var startTime = RequestHandler.GetDateTimeQueryString("startTime", required: false) ?? DateTime.UtcNow;

                BackupUtils.CheckServerHealthBeforeBackup(ServerStore, backupName);

                AssertBackup(backupConfiguration);

                var sw = Stopwatch.StartNew();
                ServerStore.ConcurrentBackupsCounter.StartBackup(RequestHandler.DatabaseName, backupName, Logger);
                try
                {
                    var cancelToken = RequestHandler.CreateBackgroundOperationToken();
                    ScheduleBackup(backupConfiguration, operationId, backupName, sw, startTime, cancelToken);

                    await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                    {
                        writer.WriteOperationIdAndNodeTag(context, operationId, ServerStore.NodeTag);
                    }

                    RequestHandler.LogTaskToAudit(Web.RequestHandler.BackupDatabaseOnceTag, operationId, json);
                }
                catch (Exception e)
                {
                    ServerStore.ConcurrentBackupsCounter.FinishBackup(RequestHandler.DatabaseName, backupName, backupStatus: null, sw.Elapsed, Logger);

                    var message = $"Failed to run backup: '{backupName}'";

                    if (Logger.IsErrorEnabled)
                        Logger.Error(message, e);

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
