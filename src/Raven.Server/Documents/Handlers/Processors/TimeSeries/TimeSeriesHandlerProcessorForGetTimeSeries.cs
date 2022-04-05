using System;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.Documents.Includes;
using Raven.Server.ServerWide.Context;
using Sparrow.Utils;

namespace Raven.Server.Documents.Handlers.Processors.TimeSeries
{
    internal class TimeSeriesHandlerProcessorForGetTimeSeries : AbstractTimeSeriesHandlerProcessorForGetTimeSeries<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public TimeSeriesHandlerProcessorForGetTimeSeries([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override ValueTask<(TimeSeriesRangeResult Result, long? TotalResults, HttpStatusCode StatusCode)> GetTimeSeriesAsync(DocumentsOperationContext context, string docId, string name, DateTime @from, DateTime to,
            int start, int pageSize, bool includeDoc,
            bool includeTags, bool fullResults)
        {
            using (context.OpenReadTransaction())
            {
                var stats = context.DocumentDatabase.DocumentsStorage.TimeSeriesStorage.Stats.GetStats(context, docId, name);
                if (stats == default)
                {
                    return ValueTask.FromResult<(TimeSeriesRangeResult Result, long? TotalResults, HttpStatusCode StatusCode)>((null, null, HttpStatusCode.NotFound));
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
                    return ValueTask.FromResult<(TimeSeriesRangeResult Result, long? TotalResults, HttpStatusCode StatusCode)>((null, null, HttpStatusCode.NotModified));
                }

                HttpContext.Response.Headers[Constants.Headers.Etag] = "\"" + hash + "\"";

                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Stav, DevelopmentHelper.Severity.Normal,
                    "Make sure etag returns in the stream to the original caller.");

                long? totalCount = null;
                if (from <= stats.Start && to >= stats.End)
                {
                    totalCount = stats.Count;
                }

                return ValueTask.FromResult((rangeResult, totalCount, HttpStatusCode.OK));
            }
        }
    }
}
