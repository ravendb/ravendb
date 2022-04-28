using System.IO;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal class OngoingTasksHandlerProcessorForGetPeriodicBackupConfiguration<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext 
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        public OngoingTasksHandlerProcessorForGetPeriodicBackupConfiguration([NotNull] TRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        public override async ValueTask ExecuteAsync()
        {
            // FullPath removes the trailing '/' so adding it back for the studio
            var localRootPath = RequestHandler.ServerStore.Configuration.Backup.LocalRootPath;
            var localRootFullPath = localRootPath != null ? localRootPath.FullPath + Path.DirectorySeparatorChar : null;
            var result = new DynamicJsonValue
            {
                [nameof(RequestHandler.ServerStore.Configuration.Backup.LocalRootPath)] = localRootFullPath,
                [nameof(RequestHandler.ServerStore.Configuration.Backup.AllowedAwsRegions)] = RequestHandler.ServerStore.Configuration.Backup.AllowedAwsRegions,
                [nameof(RequestHandler.ServerStore.Configuration.Backup.AllowedDestinations)] = RequestHandler.ServerStore.Configuration.Backup.AllowedDestinations,
            };

            using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                context.Write(writer, result);
            }
        }
    }
}
