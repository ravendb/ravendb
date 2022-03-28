using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Configuration;

internal abstract class AbstractConfigurationHandlerProcessorForGetTimeSeriesConfiguration<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractConfigurationHandlerProcessorForGetTimeSeriesConfiguration([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool)
        : base(requestHandler, contextPool)
    {
    }

    protected abstract TimeSeriesConfiguration GetTimeSeriesConfiguration();

    public override async ValueTask ExecuteAsync()
    {
        var timeSeriesConfiguration = GetTimeSeriesConfiguration();

        if (timeSeriesConfiguration == null)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
            return;
        }

        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            var val = timeSeriesConfiguration.ToJson();
            var timeSeriesConfigurationJson = context.ReadObject(val, Constants.TimeSeries.All);

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteObject(timeSeriesConfigurationJson);
            }
        }
    }
}
