using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.TimeSeries
{
    internal abstract class AbstractTimeSeriesHandlerProcessorForGetDebugSegmentsSummary<TRequestHandler, TOperationContext> : AbstractTimeSeriesHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        public AbstractTimeSeriesHandlerProcessorForGetDebugSegmentsSummary([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool) : base(requestHandler, contextPool)
        {
        }

        protected abstract ValueTask GetSegmentsSummaryAndWriteAsync(TOperationContext context, string docId, string name, DateTime from, DateTime to);

        public override async ValueTask ExecuteAsync()
        {
            var documentId = RequestHandler.GetStringQueryString("docId");
            var name = RequestHandler.GetStringQueryString("name");
            var from = RequestHandler.GetDateTimeQueryString("from", false) ?? DateTime.MinValue;
            var to = RequestHandler.GetDateTimeQueryString("to", false) ?? DateTime.MaxValue;

            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            {
                await GetSegmentsSummaryAndWriteAsync(context, documentId, name, from, to);
            }
        }
    }
}
