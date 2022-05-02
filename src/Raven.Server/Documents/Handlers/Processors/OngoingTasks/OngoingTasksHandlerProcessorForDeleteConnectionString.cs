using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Exceptions;
using Raven.Client.Util;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal class OngoingTasksHandlerProcessorForDeleteConnectionString<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext 
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {

        public OngoingTasksHandlerProcessorForDeleteConnectionString([NotNull] TRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        public override async ValueTask ExecuteAsync()
        {
            if (await RequestHandler.CanAccessDatabaseAsync(RequestHandler.DatabaseName, requireAdmin: true, requireWrite: true) == false)
                return;

            if (ResourceNameValidator.IsValidResourceName(RequestHandler.DatabaseName, RequestHandler.ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            var connectionStringName = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("connectionString");
            var type = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("type");

            await RequestHandler.ServerStore.EnsureNotPassiveAsync();

            var (index, _) = await RequestHandler.ServerStore.RemoveConnectionString(RequestHandler.DatabaseName, connectionStringName, type, RequestHandler.GetRaftRequestIdFromQuery());

            using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                await RequestHandler.WaitForIndexToBeAppliedAsync(context, index);
                RequestHandler.HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

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
