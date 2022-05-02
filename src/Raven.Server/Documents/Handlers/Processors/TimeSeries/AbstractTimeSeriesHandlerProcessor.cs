using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Platform;

namespace Raven.Server.Documents.Handlers.Processors.TimeSeries
{
    internal abstract class AbstractTimeSeriesHandlerProcessor<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext 
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractTimeSeriesHandlerProcessor([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected async Task SendConfigurationResponseAsync(TransactionOperationContext context, long index)
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                var response = new DynamicJsonValue { ["RaftCommandIndex"] = index, };
                context.Write(writer, response);
            }
        }

        public static unsafe DateTime ParseDate(string dateStr, string name)
        {
            fixed (char* c = dateStr)
            {
                var result = LazyStringParser.TryParseDateTime(c, dateStr.Length, out var dt, out _, properlyParseThreeDigitsMilliseconds: true);
                if (result != LazyStringParser.Result.DateTime)
                    Web.RequestHandler.ThrowInvalidDateTime(name, dateStr);

                return dt;
            }
        }

        public static bool CheckIfIncrementalTs(string tsName)
        {
            if (tsName.StartsWith(Constants.Headers.IncrementalTimeSeriesPrefix, StringComparison.OrdinalIgnoreCase) == false)
                return false;

            return tsName.Contains('@') == false;
        }

        internal static unsafe TimeSeriesRangeResult GetTimeSeriesRange(DocumentsOperationContext context, string docId, string name, DateTime from, DateTime to, ref int start, ref int pageSize,
            IncludeDocumentsDuringTimeSeriesLoadingCommand includesCommand = null)
        {
            if (pageSize == 0)
                return null;

            List<TimeSeriesEntry> values = new List<TimeSeriesEntry>();

            var reader = new TimeSeriesReader(context, docId, name, @from, to, offset: null);

            // init hash
            var size = Sodium.crypto_generichash_bytes();
            Debug.Assert((int)size == 32);
            var cryptoGenerichashStatebytes = (int)Sodium.crypto_generichash_statebytes();
            var state = stackalloc byte[cryptoGenerichashStatebytes];
            if (Sodium.crypto_generichash_init(state, null, UIntPtr.Zero, size) != 0)
                ComputeHttpEtags.ThrowFailToInitHash();

            var initialStart = start;
            var hasMore = false;
            DateTime lastSeenEntry = @from;

            includesCommand?.InitializeNewRangeResult(state);

            foreach (var (individualValues, segmentResult) in reader.SegmentsOrValues())
            {
                if (individualValues == null &&
                    start > segmentResult.Summary.NumberOfLiveEntries)
                {
                    lastSeenEntry = segmentResult.End;
                    start -= segmentResult.Summary.NumberOfLiveEntries;
                    continue;
                }

                var enumerable = individualValues ?? segmentResult.Values;

                foreach (var singleResult in enumerable)
                {
                    lastSeenEntry = segmentResult.End;

                    if (start-- > 0)
                        continue;

                    if (pageSize-- <= 0)
                    {
                        hasMore = true;
                        break;
                    }

                    includesCommand?.Fill(singleResult.Tag);

                    values.Add(new TimeSeriesEntry
                    {
                        Timestamp = singleResult.Timestamp,
                        Tag = singleResult.Tag,
                        Values = singleResult.Values.ToArray(),
                        IsRollup = singleResult.Type == SingleResultType.RolledUp
                    });
                }

                ComputeHttpEtags.HashChangeVector(state, segmentResult.ChangeVector);

                if (pageSize <= 0)
                    break;
            }

            var hash = ComputeHttpEtags.FinalizeHash(size, state);

            TimeSeriesRangeResult result;

            if (initialStart > 0 && values.Count == 0)
            {
                // this is a special case, because before the 'start' we might have values
                result = new TimeSeriesRangeResult
                {
                    From = lastSeenEntry,
                    To = to,
                    Entries = values.ToArray(),
                    Hash = hash
                };
            }
            else
            {
                result = new TimeSeriesRangeResult
                {
                    From = (initialStart > 0) ? values[0].Timestamp : @from,
                    To = hasMore ? values.Last().Timestamp : to,
                    Entries = values.ToArray(),
                    Hash = hash
                };
            }
            
            includesCommand?.AddIncludesToResult(result);

            return result;
        }

        internal static unsafe TimeSeriesRangeResult GetIncrementalTimeSeriesRange(DocumentsOperationContext context, string docId, string name, DateTime from, DateTime to,
            ref int start, ref int pageSize, IncludeDocumentsDuringTimeSeriesLoadingCommand includesCommand = null, bool returnFullResults = false)
        {
            if (pageSize == 0)
                return null;

            var incrementalValues = new Dictionary<long, TimeSeriesEntry>();
            var reader = new TimeSeriesReader(context, docId, name, @from, to, offset: null);
            reader.IncludeDetails();

            // init hash
            var size = Sodium.crypto_generichash_bytes();
            Debug.Assert((int)size == 32);
            var cryptoGenerichashStatebytes = (int)Sodium.crypto_generichash_statebytes();
            var state = stackalloc byte[cryptoGenerichashStatebytes];
            if (Sodium.crypto_generichash_init(state, null, UIntPtr.Zero, size) != 0)
                ComputeHttpEtags.ThrowFailToInitHash();

            var initialStart = start;
            var hasMore = false;
            DateTime lastSeenEntry = @from;

            includesCommand?.InitializeNewRangeResult(state);

            foreach (var (individualValues, segmentResult) in reader.SegmentsOrValues())
            {
                if (individualValues == null &&
                    start > segmentResult.Summary.NumberOfLiveEntries)
                {
                    lastSeenEntry = segmentResult.End;
                    start -= segmentResult.Summary.NumberOfLiveEntries;
                    continue;
                }

                var enumerable = individualValues ?? segmentResult.Values;

                foreach (var singleResult in enumerable)
                {
                    lastSeenEntry = segmentResult.End;

                    if (start-- > 0)
                        continue;

                    includesCommand?.Fill(singleResult.Tag);

                    if (pageSize-- <= 0)
                    {
                        hasMore = true;
                        break;
                    }

                    incrementalValues[singleResult.Timestamp.Ticks] = new TimeSeriesEntry
                    {
                        Timestamp = singleResult.Timestamp,
                        Values = singleResult.Values.ToArray(),
                        Tag = singleResult.Tag,
                        IsRollup = singleResult.Type == SingleResultType.RolledUp,
                        NodeValues = returnFullResults ? new Dictionary<string, double[]>(reader.GetDetails.Details) : null
                    };
                }

                ComputeHttpEtags.HashChangeVector(state, segmentResult.ChangeVector);

                if (pageSize <= 0)
                    break;
            }

            var hash = ComputeHttpEtags.FinalizeHash(size, state);

            TimeSeriesRangeResult result;

            if (initialStart > 0 && incrementalValues.Count == 0)
            {
                // this is a special case, because before the 'start' we might have values
                result = new TimeSeriesRangeResult
                {
                    From = lastSeenEntry,
                    To = to,
                    Entries = Array.Empty<TimeSeriesEntry>(),
                    Hash = hash,
                };
            }
            else
            {
                result = new TimeSeriesRangeResult
                {
                    From = (initialStart > 0) ? incrementalValues.Values.ToArray()[0].Timestamp : @from,
                    To = hasMore ? incrementalValues.Values.Last().Timestamp : to,
                    Entries = incrementalValues.Values.ToArray(),
                    Hash = hash,
                };
            }

            includesCommand?.AddIncludesToResult(result);

            return result;
        }

        internal static int WriteRange(AsyncBlittableJsonTextWriter writer, TimeSeriesRangeResult rangeResult, long? totalCount)
        {
            int size = 0;
            writer.WriteStartObject();
            {
                writer.WritePropertyName(nameof(TimeSeriesRangeResult.From));
                if (rangeResult.From == DateTime.MinValue)
                {
                    writer.WriteNull();
                }
                else
                {
                    size += writer.WriteDateTime(rangeResult.From, true);
                }
                writer.WriteComma();

                writer.WritePropertyName(nameof(TimeSeriesRangeResult.To));
                if (rangeResult.To == DateTime.MaxValue)
                {
                    writer.WriteNull();
                }
                else
                {
                    size += writer.WriteDateTime(rangeResult.To, true);
                }
                writer.WriteComma();

                writer.WritePropertyName(nameof(TimeSeriesRangeResult.Entries));
                size += WriteEntries(writer, rangeResult.Entries);

                if (totalCount.HasValue)
                {
                    // add total entries count to the response
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(TimeSeriesRangeResult.TotalResults));
                    writer.WriteInteger(totalCount.Value);
                    size += sizeof(long);
                }

                if (rangeResult.Includes != null)
                {
                    // add included documents to the response
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(TimeSeriesRangeResult.Includes));
                    writer.WriteObject(rangeResult.Includes);
                    size += rangeResult.Includes.Size;
                }

                if (rangeResult.MissingIncludes != null)
                {
                    // add included documents to the response
                    writer.WriteComma();
                    writer.WriteArray(nameof(TimeSeriesRangeResult.MissingIncludes), rangeResult.MissingIncludes);
                }
            }

            writer.WriteEndObject();

            return size;
        }

        private static int WriteEntries(AsyncBlittableJsonTextWriter writer, TimeSeriesEntry[] entries)
        {
            int size = 0;
            writer.WriteStartArray();

            for (var i = 0; i < entries.Length; i++)
            {
                if (i > 0)
                    writer.WriteComma();

                writer.WriteStartObject();
                {
                    writer.WritePropertyName(nameof(TimeSeriesEntry.Timestamp));
                    size += writer.WriteDateTime(entries[i].Timestamp, true);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(TimeSeriesEntry.Tag));
                    writer.WriteString(entries[i].Tag);
                    size += entries[i].Tag?.Length ?? 0;
                    writer.WriteComma();

                    writer.WriteArray(nameof(TimeSeriesEntry.Values), new Memory<double>(entries[i].Values));
                    size += entries[i].Values.Length * sizeof(double);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(TimeSeriesEntry.IsRollup));
                    writer.WriteBool(entries[i].IsRollup);

                    if (entries[i].NodeValues != null && entries[i].NodeValues.Count > 0)
                        WriteNodeValues(writer, entries[i].NodeValues);

                    size += 1;
                }
                writer.WriteEndObject();
            }

            writer.WriteEndArray();

            return size;
        }

        private static void WriteNodeValues(AsyncBlittableJsonTextWriter writer, Dictionary<string, double[]> nodeValues)
        {
            writer.WriteComma();
            writer.WritePropertyName(nameof(TimeSeriesEntry.NodeValues));
            writer.WriteStartObject();
            
            int i = nodeValues.Count;
            foreach (var value in nodeValues)
            {
                writer.WriteArray(value.Key, new Memory<double>(value.Value));

                if (--i > 0)
                    writer.WriteComma();
            }
            writer.WriteEndObject();
        }
    }
}
