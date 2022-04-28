using System;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Server.Documents.Includes;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Handlers.Processors.TimeSeries
{
    internal class TimeSeriesHandlerProcessorForGetTimeSeries : AbstractTimeSeriesHandlerProcessorForGetTimeSeries<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public TimeSeriesHandlerProcessorForGetTimeSeries([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask GetTimeSeriesAsync(DocumentsOperationContext context, string docId, string name, DateTime @from, DateTime to,
            int start, int pageSize, bool includeDoc,
            bool includeTags, bool fullResults)
        {
            using (context.OpenReadTransaction())
            {
                var stats = context.DocumentDatabase.DocumentsStorage.TimeSeriesStorage.Stats.GetStats(context, docId, name);
                if (stats == default)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return;
                }

                var includesCommand = includeDoc || includeTags
                    ? new IncludeDocumentsDuringTimeSeriesLoadingCommand(context, docId, includeDoc, includeTags)
                    : null;

                bool incrementalTimeSeries = CheckIfIncrementalTs(name);

                var rangeResult = incrementalTimeSeries
                    ? GetIncrementalTimeSeriesRange(context, docId, name, from, to, ref start, ref pageSize, includesCommand, fullResults)
                    : GetTimeSeriesRange(context, docId, name, from, to, ref start, ref pageSize, includesCommand);

                var hash = rangeResult?.Hash ?? string.Empty;

                var etag = RequestHandler.GetStringFromHeaders("If-None-Match");
                if (etag == hash)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                    return;
                }

                HttpContext.Response.Headers[Constants.Headers.Etag] = "\"" + hash + "\"";

                long? totalCount = null;
                if (from <= stats.Start && to >= stats.End)
                {
                    totalCount = stats.Count;
                }

                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    WriteRange(writer, rangeResult, totalCount);
                }
            }
        }
    }
}
