using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Revisions
{
    internal abstract class AbstractRevisionsHandlerProcessorForGetRevisionsConfiguration<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        public AbstractRevisionsHandlerProcessorForGetRevisionsConfiguration([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool) : base(requestHandler, contextPool)
        {
        }

        protected abstract RevisionsConfiguration GetRevisionsConfiguration(TOperationContext context);

        public override async ValueTask ExecuteAsync()
        {
            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            {
                var revisionsConfig = GetRevisionsConfiguration(context);

                if (revisionsConfig != null)
                {
                    var revisionsCollection = new DynamicJsonValue();
                    foreach (var collection in revisionsConfig.Collections)
                    {
                        revisionsCollection[collection.Key] = collection.Value.ToJson();
                    }

                    await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                    {
                        context.Write(writer, new DynamicJsonValue
                        {
                            [nameof(revisionsConfig.Default)] = revisionsConfig.Default?.ToJson(),
                            [nameof(revisionsConfig.Collections)] = revisionsCollection
                        });
                    }
                }
                else
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                }
            }
        }
    }
}
