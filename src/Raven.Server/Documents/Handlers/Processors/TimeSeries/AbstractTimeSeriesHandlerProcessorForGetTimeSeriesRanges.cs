using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.TimeSeries
{
    internal abstract class AbstractTimeSeriesHandlerProcessorForGetTimeSeriesRanges<TRequestHandler, TOperationContext> : AbstractTimeSeriesHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
        where TOperationContext : JsonOperationContext
    {
        public AbstractTimeSeriesHandlerProcessorForGetTimeSeriesRanges([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract ValueTask GetTimeSeriesRangesAndWriteAsync(TOperationContext context, string documentId, StringValues names, StringValues fromList, StringValues toList, int start, int pageSize,
            bool includeDoc, bool includeTags, bool returnFullResults, CancellationToken token);

        public override async ValueTask ExecuteAsync()
        {
            var documentId = RequestHandler.GetStringQueryString("docId");
            var names = RequestHandler.GetStringValuesQueryString("name");
            var fromList = RequestHandler.GetStringValuesQueryString("from");
            var toList = RequestHandler.GetStringValuesQueryString("to");

            var start = RequestHandler.GetStart();
            var pageSize = RequestHandler.GetPageSize();

            var includeDoc = RequestHandler.GetBoolValueQueryString("includeDocument", required: false) ?? false;
            var includeTags = RequestHandler.GetBoolValueQueryString("includeTags", required: false) ?? false;
            var returnFullResults = RequestHandler.GetBoolValueQueryString("full", required: false) ?? false;

            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            using(var token = RequestHandler.CreateOperationToken())
            {
                await GetTimeSeriesRangesAndWriteAsync(context, documentId, names, fromList, toList, start, pageSize, includeDoc, includeTags, returnFullResults, token.Token);
            }
        }

        protected async Task WriteTimeSeriesDetails(JsonOperationContext writeContext, DocumentsOperationContext docsContext, string documentId, TimeSeriesDetails ranges, CancellationToken token)
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(writeContext, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();
                {
                    writer.WritePropertyName(nameof(TimeSeriesDetails.Id));
                    writer.WriteString(ranges.Id);

                    writer.WriteComma();
                    writer.WritePropertyName(nameof(TimeSeriesDetails.Values));
                    await WriteTimeSeriesRangeResultsAsync(docsContext, writer, documentId, ranges.Values, calcTotalCount: false, token);
                }
                writer.WriteEndObject();
            }
        }

        internal static async ValueTask<int> WriteTimeSeriesRangeResultsAsync(DocumentsOperationContext context, AsyncBlittableJsonTextWriter writer, string documentId, 
            Dictionary<string, List<TimeSeriesRangeResult>> dictionary, bool calcTotalCount = false, CancellationToken token = default)
        {
            if (dictionary == null)
            {
                writer.WriteNull();
                return 0;
            }

            writer.WriteStartObject();

            int size = 0;
            bool first = true;
            foreach (var (name, ranges) in dictionary)
            {
                if (first == false)
                    writer.WriteComma();

                first = false;

                writer.WritePropertyName(name);
                size += name.Length;

                writer.WriteStartArray();

                (long Count, DateTime Start, DateTime End) stats = default;
                if (documentId != null)
                {
                    Debug.Assert(context != null);
                    stats = context.DocumentDatabase.DocumentsStorage.TimeSeriesStorage.Stats.GetStats(context, documentId, name);
                }

                for (var i = 0; i < ranges.Count; i++)
                {
                    long? totalCount = null;

                    if (i > 0)
                        writer.WriteComma();

                    if (calcTotalCount == false)
                        totalCount = ranges[i].TotalResults;
                    else if (stats != default && ranges[i].From <= stats.Start && ranges[i].To >= stats.End)
                    {
                        totalCount = stats.Count;
                    }

                    size += TimeSeriesHandlerProcessorForGetTimeSeries.WriteRange(writer, ranges[i], totalCount);

                    await writer.MaybeFlushAsync(token);
                }
                writer.WriteEndArray();
            }

            writer.WriteEndObject();
            return size;
        }

        protected static List<TimeSeriesRange> ConvertAndValidateMultipleTimeSeriesParameters(string documentId, StringValues names, StringValues fromList, StringValues toList)
        {
            if (fromList.Count == 0)
                throw new ArgumentException("Length of query string values 'from' must be greater than zero");

            if (fromList.Count != toList.Count)
                throw new ArgumentException("Length of query string values 'from' must be equal to the length of query string values 'to'");

            if (fromList.Count != names.Count)
                throw new InvalidOperationException($"GetMultipleTimeSeriesOperation : Argument count miss match on document '{documentId}'. " +
                                                    $"Received {names.Count} 'name' arguments, and {fromList.Count} 'from'/'to' arguments.");

            var ranges = new List<TimeSeriesRange>();
            for (int i = 0; i < fromList.Count; i++)
            {
                var name = names[i];

                if (string.IsNullOrEmpty(name))
                    throw new InvalidOperationException($"GetMultipleTimeSeriesOperation : Missing '{nameof(TimeSeriesRange.Name)}' argument in 'TimeSeriesRange' on document '{documentId}'. " +
                                                        $"'{nameof(TimeSeriesRange.Name)}' cannot be null or empty");

                ranges.Add(new TimeSeriesRange()
                {
                    Name = name,
                    From = string.IsNullOrEmpty(fromList[i]) ? DateTime.MinValue : TimeSeriesHandlerProcessorForGetTimeSeries.ParseDate(fromList[i], name),
                    To = string.IsNullOrEmpty(toList[i]) ? DateTime.MaxValue : TimeSeriesHandlerProcessorForGetTimeSeries.ParseDate(toList[i], name)
                });
            }

            return ranges;
        }
    }
}
