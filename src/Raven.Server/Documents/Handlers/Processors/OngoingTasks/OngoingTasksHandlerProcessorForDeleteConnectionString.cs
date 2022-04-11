using System;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Exceptions;
using Raven.Client.Util;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal class OngoingTasksHandlerProcessorForDeleteConnectionString<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        private readonly string _databaseName;

        public OngoingTasksHandlerProcessorForDeleteConnectionString([NotNull] TRequestHandler requestHandler,
            [NotNull] JsonContextPoolBase<TOperationContext> contextPool, [NotNull] string databaseName)
            : base(requestHandler, contextPool)
        {
            _databaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));
        }

        public override async ValueTask ExecuteAsync()
        {
            if (await RequestHandler.CanAccessDatabaseAsync(_databaseName, requireAdmin: true, requireWrite: true) == false)
                return;

            if (ResourceNameValidator.IsValidResourceName(_databaseName, RequestHandler.ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            var connectionStringName = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("connectionString");
            var type = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("type");

            await RequestHandler.ServerStore.EnsureNotPassiveAsync();

            var (index, _) = await RequestHandler.ServerStore.RemoveConnectionString(_databaseName, connectionStringName, type, RequestHandler.GetRaftRequestIdFromQuery());
            await RequestHandler.ServerStore.Cluster.WaitForIndexNotification(index);
            RequestHandler.HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

            using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        ["RaftCommandIndex"] = index
                    });
                }
            }
        }
    }
}
