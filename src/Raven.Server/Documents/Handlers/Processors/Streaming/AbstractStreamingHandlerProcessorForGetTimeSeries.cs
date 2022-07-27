using System;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.TimeSeries;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Streaming
{
    internal abstract class AbstractStreamingHandlerProcessorForGetTimeSeries<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
        where TOperationContext : JsonOperationContext
    {
        protected AbstractStreamingHandlerProcessorForGetTimeSeries([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract IDisposable OpenReadTransaction(TOperationContext context);

        protected abstract ValueTask GetAndWriteTimeSeriesAsync(TOperationContext context, string docId, string name, DateTime from, DateTime to, TimeSpan? offset, CancellationToken token);

        public override async ValueTask ExecuteAsync()
        {
            var documentId = RequestHandler.GetStringQueryString("docId");
            var name = RequestHandler.GetStringQueryString("name");
            var fromStr = RequestHandler.GetStringQueryString("from", required: false);
            var toStr = RequestHandler.GetStringQueryString("to", required: false);
            var offset = RequestHandler.GetTimeSpanQueryString("offset", required: false);

            var from = string.IsNullOrEmpty(fromStr)
                ? DateTime.MinValue
                : TimeSeriesHandlerProcessorForGetTimeSeries.ParseDate(fromStr, name);

            var to = string.IsNullOrEmpty(toStr)
                ? DateTime.MaxValue
                : TimeSeriesHandlerProcessorForGetTimeSeries.ParseDate(toStr, name);

            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            using (OpenReadTransaction(context))
            using (var token = RequestHandler.CreateOperationToken())
            {
                await GetAndWriteTimeSeriesAsync(context, documentId, name, from, to, offset, token.Token);
            }
        }
    }
}
