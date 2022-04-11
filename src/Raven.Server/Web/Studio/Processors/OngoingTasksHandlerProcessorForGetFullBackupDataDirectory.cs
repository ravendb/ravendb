using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Config.Settings;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.Sharding.Handlers;
using Sparrow.Json;

namespace Raven.Server.Web.Studio.Processors
{
    internal class OngoingTasksHandlerProcessorForGetFullBackupDataDirectory<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        public OngoingTasksHandlerProcessorForGetFullBackupDataDirectory([NotNull] TRequestHandler requestHandler,
            [NotNull] JsonContextPoolBase<TOperationContext> contextPool)
            : base(requestHandler, contextPool)
        {
        }

        private string GetDatabaseName()
        {
            return RequestHandler switch
            {
                ShardedDatabaseRequestHandler sharded => sharded.DatabaseContext.DatabaseName,
                DatabaseRequestHandler database => database.Database.Name,
                _ => null
            };
        }

        public override async ValueTask ExecuteAsync()
        {
            var databaseName = GetDatabaseName();

            var path = RequestHandler.GetStringQueryString("path", required: true);
            var requestTimeoutInMs = RequestHandler.GetIntValueQueryString("requestTimeoutInMs", required: false) ?? 5 * 1000;
            var getNodesInfo = RequestHandler.GetBoolValueQueryString("getNodesInfo", required: false) ?? false;

            var pathSetting = new PathSetting(path);
            await BackupConfigurationHelper.GetFullBackupDataDirectory(pathSetting, databaseName, requestTimeoutInMs, getNodesInfo, RequestHandler.ServerStore, RequestHandler.ResponseBodyStream());
        }
    }
}
