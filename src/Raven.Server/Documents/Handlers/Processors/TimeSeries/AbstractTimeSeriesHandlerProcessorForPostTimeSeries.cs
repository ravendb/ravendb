using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.TrafficWatch;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.TimeSeries
{
    internal abstract class AbstractTimeSeriesHandlerProcessorForPostTimeSeries<TRequestHandler, TOperationContext> : AbstractTimeSeriesHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext 
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractTimeSeriesHandlerProcessorForPostTimeSeries([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract ValueTask ApplyTimeSeriesOperationAsync(string docId, TimeSeriesOperation operation, TOperationContext context);

        public override async ValueTask ExecuteAsync()
        {
            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            {
                var documentId = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("docId");

                var blittable = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "timeseries");
                var operation = TimeSeriesOperation.Parse(blittable);

                if (TrafficWatchManager.HasRegisteredClients)
                    RequestHandler.AddStringToHttpContext(blittable.ToString(), TrafficWatchChangeType.TimeSeries);

                await ApplyTimeSeriesOperationAsync(documentId, operation, context);
            }
        }
    }
}
