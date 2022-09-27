using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal abstract class AbstractOngoingTasksHandlerProcessorForUpdatePeriodicBackup<TRequestHandler, TOperationContext> : AbstractHandlerProcessorForUpdateDatabaseConfiguration<PeriodicBackupConfiguration, TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractOngoingTasksHandlerProcessorForUpdatePeriodicBackup([NotNull] TRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected override async ValueTask<PeriodicBackupConfiguration> GetConfigurationAsync(TransactionOperationContext context, AsyncBlittableJsonTextWriter writer)
        {
            var json = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), GetType().Name);

            return JsonDeserializationCluster.PeriodicBackupConfiguration(json);
        }

        protected override void OnBeforeUpdateConfiguration(ref PeriodicBackupConfiguration configuration, JsonOperationContext context)
        {
            BackupConfigurationHelper.AssertPeriodicBackup(configuration, RequestHandler.ServerStore);
        }

        protected override void OnBeforeResponseWrite(TransactionOperationContext _, DynamicJsonValue responseJson, PeriodicBackupConfiguration configuration, long index)
        {
            const string taskIdName = nameof(PeriodicBackupConfiguration.TaskId);

            var taskId = configuration.TaskId;
            if (taskId == 0)
                taskId = index;
            
            responseJson[taskIdName] = taskId;
        }

        protected override Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context, PeriodicBackupConfiguration configuration, string raftRequestId)
        {
            return RequestHandler.ServerStore.ModifyPeriodicBackup(context, RequestHandler.DatabaseName, configuration, raftRequestId);
        }
    }
}
