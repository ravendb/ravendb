using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.TimeSeries
{
    internal abstract class AbstractTimeSeriesHandlerProcessorForGetTimeSeries<TRequestHandler, TOperationContext> : AbstractTimeSeriesHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext 
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractTimeSeriesHandlerProcessorForGetTimeSeries([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract ValueTask<TimeSeriesRangeResult> GetTimeSeriesAndWriteAsync(TOperationContext context, string docId, string name, DateTime from, DateTime to, int start,
            int pageSize, bool includeDoc, bool includeTags, bool fullResults);
        
        public override async ValueTask ExecuteAsync()
        {
            var documentId = RequestHandler.GetStringQueryString("docId");
            var name = RequestHandler.GetStringQueryString("name");
            var fromStr = RequestHandler.GetStringQueryString("from", required: false);
            var toStr = RequestHandler.GetStringQueryString("to", required: false);

            var start = RequestHandler.GetStart();
            var pageSize = RequestHandler.GetPageSize();

            var includeDoc = RequestHandler.GetBoolValueQueryString("includeDocument", required: false) ?? false;
            var includeTags = RequestHandler.GetBoolValueQueryString("includeTags", required: false) ?? false;
            var fullResults = RequestHandler.GetBoolValueQueryString("full", required: false) ?? false;

            var from = string.IsNullOrEmpty(fromStr)
                ? DateTime.MinValue
                : ParseDate(fromStr, name);

            var to = string.IsNullOrEmpty(toStr)
                ? DateTime.MaxValue
                : ParseDate(toStr, name);

            TimeSeriesRangeResult rangeResult;
            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            {
                rangeResult = await GetTimeSeriesAndWriteAsync(context, documentId, name, from, to, start, pageSize, includeDoc, includeTags, fullResults);

                if (rangeResult == null)
                    return; // not-modified or not-found

                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    WriteRange(writer, rangeResult, rangeResult?.TotalResults);
                }
            }
        }
    }
}
