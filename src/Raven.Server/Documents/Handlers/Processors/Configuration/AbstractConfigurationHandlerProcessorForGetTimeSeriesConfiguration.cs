using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Configuration;

internal abstract class AbstractConfigurationHandlerProcessorForGetTimeSeriesConfiguration<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext 
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractConfigurationHandlerProcessorForGetTimeSeriesConfiguration([NotNull] TRequestHandler requestHandler) : base(requestHandler)
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
