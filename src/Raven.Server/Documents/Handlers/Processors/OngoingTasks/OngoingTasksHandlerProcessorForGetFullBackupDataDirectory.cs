using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.PeriodicBackup;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal class OngoingTasksHandlerProcessorForGetFullBackupDataDirectory<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext 
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {

        public OngoingTasksHandlerProcessorForGetFullBackupDataDirectory([NotNull] TRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        public override async ValueTask ExecuteAsync()
        {
            var path = RequestHandler.GetStringQueryString("path", required: true);
            var requestTimeoutInMs = RequestHandler.GetIntValueQueryString("requestTimeoutInMs", required: false) ?? 5 * 1000;
            var getNodesInfo = RequestHandler.GetBoolValueQueryString("getNodesInfo", required: false) ?? false;

            var pathSetting = new PathSetting(path);
            await BackupConfigurationHelper.GetFullBackupDataDirectory(pathSetting, RequestHandler.DatabaseName, requestTimeoutInMs, getNodesInfo, RequestHandler.ServerStore, RequestHandler.ResponseBodyStream());
        }
    }
}
