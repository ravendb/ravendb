using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.Extensions.Primitives;
using Raven.Client;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.Documents.Includes;
using Raven.Server.ServerWide.Context;
using Sparrow.Platform;

namespace Raven.Server.Documents.Handlers.Processors.TimeSeries
{
    internal class TimeSeriesHandlerProcessorForGetTimeSeriesRanges : AbstractTimeSeriesHandlerProcessorForGetTimeSeriesRanges<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public TimeSeriesHandlerProcessorForGetTimeSeriesRanges([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask GetTimeSeriesRangesAndWriteAsync(DocumentsOperationContext context, string documentId, StringValues names, StringValues fromList, StringValues toList, int start, int pageSize,
            bool includeDoc, bool includeTags, bool returnFullResults, CancellationToken token)
        {
            using (context.OpenReadTransaction())
            {
                (long Count, DateTime Start, DateTime End) stats;
                foreach (var name in names)
                {
                    stats = context.DocumentDatabase.DocumentsStorage.TimeSeriesStorage.Stats.GetStats(context, documentId, name);
                    if (stats == default)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                        return;
                    }
                }

                bool shouldGetMissingIncludes = RequestHandler.GetBoolFromHeaders(Constants.Headers.Sharded) ?? false;

                var includesCommand = includeDoc || includeTags
                    ? new IncludeDocumentsDuringTimeSeriesLoadingCommand(context, documentId, includeDoc, includeTags, shouldGetMissingIncludes)
                    : null;

                var ranges = GetTimeSeriesRangeResults(context, documentId, names, fromList, toList, start, pageSize, includesCommand, returnFullResults);
                
                var actualEtag = CombineHashesFromMultipleRanges(ranges);

                var etag = RequestHandler.GetStringFromHeaders(Constants.Headers.IfNoneMatch);
                if (etag == actualEtag)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                    return;
                }

                HttpContext.Response.Headers[Constants.Headers.Etag] = "\"" + actualEtag + "\"";

                var result = new TimeSeriesDetails() {Id = documentId, Values = ranges};

                await WriteTimeSeriesDetails(writeContext: context, docsContext: context, documentId, result, token);
            }
        }

        private static Dictionary<string, List<TimeSeriesRangeResult>> GetTimeSeriesRangeResults(DocumentsOperationContext context, string documentId, StringValues names, StringValues fromList, StringValues toList, int start, int pageSize,
            IncludeDocumentsDuringTimeSeriesLoadingCommand includes, bool returnFullResult = false)
        {
            var ranges = ConvertAndValidateMultipleTimeSeriesParameters(documentId, names, fromList, toList);
            var rangeResultDictionary = new Dictionary<string, List<TimeSeriesRangeResult>>(StringComparer.OrdinalIgnoreCase);

            foreach (var range in ranges)
            {
                bool incrementalTimeSeries = TimeSeriesHandlerProcessorForGetTimeSeries.CheckIfIncrementalTs(range.Name);
                
                var rangeResult = incrementalTimeSeries ?
                    TimeSeriesHandlerProcessorForGetTimeSeries.GetIncrementalTimeSeriesRange(context, documentId, range.Name, range.From ?? DateTime.MinValue, range.To ?? DateTime.MaxValue, ref start, ref pageSize, includes, returnFullResult) :
                    TimeSeriesHandlerProcessorForGetTimeSeries.GetTimeSeriesRange(context, documentId, range.Name, range.From ?? DateTime.MinValue, range.To ?? DateTime.MaxValue, ref start, ref pageSize, includes);

                if (rangeResult == null)
                {
                    Debug.Assert(pageSize <= 0, "Page size must be zero or less here");
                    return rangeResultDictionary;
                }
                if (rangeResultDictionary.TryGetValue(range.Name, out var list) == false)
                {
                    rangeResultDictionary[range.Name] = new List<TimeSeriesRangeResult> { rangeResult };
                }
                else
                {
                    list.Add(rangeResult);
                }

                if (pageSize <= 0)
                    break;
            }

            return rangeResultDictionary;
        }

        private static unsafe string CombineHashesFromMultipleRanges(Dictionary<string, List<TimeSeriesRangeResult>> ranges)
        {
            // init hash
            var size = Sodium.crypto_generichash_bytes();
            Debug.Assert((int)size == 32);
            var cryptoGenerichashStatebytes = (int)Sodium.crypto_generichash_statebytes();
            var state = stackalloc byte[cryptoGenerichashStatebytes];
            if (Sodium.crypto_generichash_init(state, null, UIntPtr.Zero, size) != 0)
                ComputeHttpEtags.ThrowFailToInitHash();

            ComputeHttpEtags.HashNumber(state, ranges.Count);

            foreach (var kvp in ranges)
            {
                foreach (var range in kvp.Value)
                {
                    ComputeHttpEtags.HashChangeVector(state, range.Hash);
                }
            }

            return ComputeHttpEtags.FinalizeHash(size, state);
        }
    }
}
