using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.Http;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Impl;

namespace Raven.Server.Documents.Handlers.Processors.Debugging
{
    internal sealed class StorageHandlerProcessorForGetScratchBufferReport : AbstractStorageHandlerProcessorForGetEnvironmentReport<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public StorageHandlerProcessorForGetScratchBufferReport([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override bool SupportsCurrentNode => true;

        protected override async ValueTask HandleCurrentNodeAsync()
        {
            var name = GetName();
            var typeAsString = RequestHandler.GetStringQueryString("type", false);

            var envs = RequestHandler.Database.GetAllStoragesEnvironment();

            if (typeAsString != null)
            {
                if (Enum.TryParse(typeAsString, out StorageEnvironmentWithType.StorageEnvironmentType type) == false)
                    throw new InvalidOperationException("Query string value 'type' is not a valid environment type: " + typeAsString);
                var env = envs.FirstOrDefault(x => string.Equals(x.Name, name, StringComparison.OrdinalIgnoreCase) && x.Type == type);
                if (env == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return;
                }

                using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                    {
                        WriteReport(writer, env, context);
                    }
                }
            }
            else
            {
                using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                    {
                        WriteEnvironmentsReport(writer, name, envs, context);
                    }
                }
            }
        }

        protected override Task HandleRemoteNodeAsync(ProxyCommand<object> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);

        protected override DynamicJsonValue GetJsonReport(StorageEnvironmentWithType env, LowLevelTransaction lowTx, bool de)
        {
            //Opening a write transaction to avoid concurrency problems (Issue #21088)
            var sc = env.Environment.ScratchBufferPool.InfoForDebug(env.Environment.PossibleOldestReadTransaction(lowTx));
            return (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(sc);
        }
    }
}
