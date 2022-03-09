using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Server.Web;
using Raven.Server.Web.Studio.Sharding.Processors;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors;

internal abstract class AbstractConfigurationHandlerProcessorForGetStudioConfiguration<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractConfigurationHandlerProcessorForGetStudioConfiguration([NotNull] TRequestHandler requestHandler,[NotNull] JsonContextPoolBase<TOperationContext> contextPool)
        : base(requestHandler, contextPool)
    {
    }

    protected abstract StudioConfiguration GetStudioConfiguration();

    public override async ValueTask ExecuteAsync()
    {
        var studioConfiguration = GetStudioConfiguration();

        if (studioConfiguration == null)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
            return;
        }

        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            var val = studioConfiguration.ToJson();
            var clientConfigurationJson = context.ReadObject(val, Constants.Configuration.StudioId);

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteObject(clientConfigurationJson);
            }
        }
    }
}
