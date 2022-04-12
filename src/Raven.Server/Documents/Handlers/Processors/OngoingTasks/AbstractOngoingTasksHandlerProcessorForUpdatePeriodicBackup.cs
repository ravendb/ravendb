using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal abstract class AbstractOngoingTasksHandlerProcessorForUpdatePeriodicBackup<TRequestHandler> : AbstractHandlerProcessorForUpdateDatabaseConfiguration<BlittableJsonReaderObject, TRequestHandler>
        where TRequestHandler : RequestHandler
    {
        protected AbstractOngoingTasksHandlerProcessorForUpdatePeriodicBackup([NotNull] TRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected override void OnBeforeUpdateConfiguration(ref BlittableJsonReaderObject configuration, JsonOperationContext context)
        {
            var periodicBackupConfiguration = JsonDeserializationCluster.PeriodicBackupConfiguration(configuration);

            RequestHandler.ServerStore.LicenseManager.AssertCanAddPeriodicBackup(periodicBackupConfiguration);
            BackupConfigurationHelper.UpdateLocalPathIfNeeded(periodicBackupConfiguration, RequestHandler.ServerStore);
            BackupConfigurationHelper.AssertBackupConfiguration(periodicBackupConfiguration);
            BackupConfigurationHelper.AssertDestinationAndRegionAreAllowed(periodicBackupConfiguration, RequestHandler.ServerStore);

            configuration = context.ReadObject(periodicBackupConfiguration.ToJson(), "updated-backup-configuration");
        }

        protected override void OnBeforeResponseWrite(DynamicJsonValue responseJson, BlittableJsonReaderObject configuration, long index)
        {
            const string taskIdName = nameof(PeriodicBackupConfiguration.TaskId);

            configuration.TryGet(taskIdName, out long taskId);
            if (taskId == 0)
                taskId = index;
            
            responseJson[taskIdName] = taskId;
        }

        protected override Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context, string databaseName, BlittableJsonReaderObject configuration, string raftRequestId)
        {
            return RequestHandler.ServerStore.ModifyPeriodicBackup(context, databaseName, configuration, raftRequestId);
        }
    }
}
