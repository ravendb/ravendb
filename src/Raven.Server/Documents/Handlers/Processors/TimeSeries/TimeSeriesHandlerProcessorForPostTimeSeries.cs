using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Exceptions.Documents;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.TimeSeries
{
    internal class TimeSeriesHandlerProcessorForPostTimeSeries : AbstractTimeSeriesHandlerProcessorForPostTimeSeries<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public TimeSeriesHandlerProcessorForPostTimeSeries([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override async ValueTask ApplyTimeSeriesOperationAsync(string docId, TimeSeriesOperation operation, DocumentsOperationContext _)
        {
            var cmd = new TimeSeriesHandler.ExecuteTimeSeriesBatchCommand(RequestHandler.Database, docId, operation, fromEtl: false);

            try
            {
                await RequestHandler.Database.TxMerger.Enqueue(cmd);
                RequestHandler.NoContentStatus();
            }
            catch (DocumentDoesNotExistException)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                throw;
            }
        }
    }
}
