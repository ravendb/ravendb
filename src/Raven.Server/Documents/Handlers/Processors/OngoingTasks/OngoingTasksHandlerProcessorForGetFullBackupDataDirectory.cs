using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal class OngoingTasksHandlerProcessorForGetFullBackupDataDirectory<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        private readonly string _databaseName;

        public OngoingTasksHandlerProcessorForGetFullBackupDataDirectory([NotNull] TRequestHandler requestHandler,
            [NotNull] JsonContextPoolBase<TOperationContext> contextPool, [NotNull] string databaseName)
            : base(requestHandler, contextPool)
        {
            _databaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));
        }

        public override async ValueTask ExecuteAsync()
        {
            var path = RequestHandler.GetStringQueryString("path", required: true);
            var requestTimeoutInMs = RequestHandler.GetIntValueQueryString("requestTimeoutInMs", required: false) ?? 5 * 1000;
            var getNodesInfo = RequestHandler.GetBoolValueQueryString("getNodesInfo", required: false) ?? false;

            var pathSetting = new PathSetting(path);
            await BackupConfigurationHelper.GetFullBackupDataDirectory(pathSetting, _databaseName, requestTimeoutInMs, getNodesInfo, RequestHandler.ServerStore, RequestHandler.ResponseBodyStream());
        }
    }
}
