using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Revisions
{
    internal abstract class AbstractRevisionsHandlerProcessorForGetRevisionsConflictsConfiguration<TRequestHandler> : AbstractHandlerProcessor<TRequestHandler>
        where TRequestHandler : RequestHandler
    {
        protected AbstractRevisionsHandlerProcessorForGetRevisionsConflictsConfiguration([NotNull] TRequestHandler requestHandler) 
            : base(requestHandler)
        {
        }

        protected abstract RevisionsCollectionConfiguration GetRevisionsConflicts();

        public override async ValueTask ExecuteAsync()
        {
            var revisionsForConflictsConfig = GetRevisionsConflicts();

            if (revisionsForConflictsConfig != null)
            {
                using (ClusterContextPool.AllocateOperationContext(out JsonOperationContext context))
                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    context.Write(writer, revisionsForConflictsConfig.ToJson());
                }
            }
            else
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
            }
        }
    }
}
