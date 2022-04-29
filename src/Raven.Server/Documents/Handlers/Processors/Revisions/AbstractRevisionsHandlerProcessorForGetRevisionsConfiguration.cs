using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Revisions
{
    internal abstract class AbstractRevisionsHandlerProcessorForGetRevisionsConfiguration<TRequestHandler> : AbstractHandlerProcessor<TRequestHandler>
        where TRequestHandler : RequestHandler
    {
        protected AbstractRevisionsHandlerProcessorForGetRevisionsConfiguration([NotNull] TRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected abstract RevisionsConfiguration GetRevisionsConfiguration();

        public override async ValueTask ExecuteAsync()
        {

            var revisionsConfig = GetRevisionsConfiguration();

            if (revisionsConfig != null)
            {
                var revisionsCollection = new DynamicJsonValue();
                foreach (var collection in revisionsConfig.Collections)
                {
                    revisionsCollection[collection.Key] = collection.Value.ToJson();
                }

                using (ClusterContextPool.AllocateOperationContext(out JsonOperationContext context))
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
