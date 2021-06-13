//-----------------------------------------------------------------------
// <copyright file="AsyncSessionDocumentTimeSeries.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Client.Documents.Session.Loaders;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Documents.Session.TimeSeries;
using Sparrow;
using Sparrow.Json;

namespace Raven.Client.Documents.Session
{
    public class AsyncSessionDocumentTimeSeries<TValues> : SessionTimeSeriesBase, IAsyncSessionDocumentTimeSeries, IAsyncSessionDocumentRollupTypedTimeSeries<TValues>, IAsyncSessionDocumentTypedTimeSeries<TValues> where TValues : new()
    {
        public AsyncSessionDocumentTimeSeries(InMemoryDocumentSessionOperations session, string documentId, string name) : base(session, documentId, name)
        {
        }

        public AsyncSessionDocumentTimeSeries(InMemoryDocumentSessionOperations session, object entity, string name) : base(session, entity, name)
        {
        }

        public Task<TimeSeriesEntry[]> GetAsync(DateTime? from = null, DateTime? to = null, int start = 0, int pageSize = int.MaxValue, CancellationToken token = default)
        {
            return GetAsync(from, to, includes: null, start, pageSize, token);
        }

        public async Task<TimeSeriesEntry[]> GetAsync(DateTime? from, DateTime? to, Action<ITimeSeriesIncludeBuilder> includes, int start = 0, int pageSize = int.MaxValue, CancellationToken token = default) 
        {
            if (NotInCache(from, to))
            {
                return await GetTimeSeriesAndIncludes<TimeSeriesEntry>(from, to, includes, start, pageSize, token)
                    .ConfigureAwait(false);
            }

            var resultToUser =
                await ServeFromCache(from ?? DateTime.MinValue, to ?? DateTime.MaxValue, start, pageSize, includes, token)
                    .ConfigureAwait(false);

            return resultToUser?.Take(pageSize).ToArray();
        }

        internal async Task<TimeSeriesEntry<TEntry>[]> GetTypedFromCache<TEntry>(DateTime? from, DateTime? to, Action<ITimeSeriesIncludeBuilder> includes, int start,
            int pageSize, CancellationToken token = default) where TEntry : new()
        {
            // RavenDB-16060 
            // Typed TimeSeries results need special handling when served from cache
            // since we cache the results as non-typed 

            var resultToUser =
                await ServeFromCache(from ?? DateTime.MinValue, to ?? DateTime.MaxValue, start, pageSize, includes, token)
                    .ConfigureAwait(false);

            var asList = resultToUser.ToList();
            if (asList.Count == 0)
                return Array.Empty<TimeSeriesEntry<TEntry>>();

            var result = new TimeSeriesEntry<TEntry>[asList.Count];

            for (var index = 0; index < asList.Count; index++)
            {
                var timeSeriesEntry = new TimeSeriesEntry<TEntry>();

                var item = asList[index];

                timeSeriesEntry.IsRollup = item.IsRollup;
                timeSeriesEntry.Timestamp = item.Timestamp;
                timeSeriesEntry.Tag = item.Tag;
                timeSeriesEntry.Value = TimeSeriesValuesHelper.SetMembers<TEntry>(item.Values, item.IsRollup);
                timeSeriesEntry.Values = item.Values;

                result[index] = timeSeriesEntry;
            }

            return result;
        }

        internal bool NotInCache(DateTime? from, DateTime? to)
        {
            return Session.TimeSeriesByDocId.TryGetValue(DocId, out var cache) == false ||
                   cache.TryGetValue(Name, out var ranges) == false ||
                   ranges.Count == 0 ||
                   ranges[0].From > to || 
                   ranges[ranges.Count - 1].To < from;
        }

        internal async Task<TTValues[]> GetTimeSeriesAndIncludes<TTValues>(DateTime? from, DateTime? to, Action<ITimeSeriesIncludeBuilder> includes, int start, int pageSize, CancellationToken token = default) where TTValues : TimeSeriesEntry
        {
            from = from?.EnsureUtc();
            to = to?.EnsureUtc();

            if (pageSize == 0)
                return Array.Empty<TTValues>();

            if (Session.DocumentsById.TryGetValue(DocId, out var document) &&
                document.Metadata.TryGet(Constants.Documents.Metadata.TimeSeries, out BlittableJsonReaderArray metadataTimeSeries) &&
                metadataTimeSeries.BinarySearch(Name, StringComparison.OrdinalIgnoreCase) < 0)
            {
                // the document is loaded in the session, but the metadata says that there is no such timeseries
                return Array.Empty<TTValues>();
            }

            Session.IncrementRequestCount();

            var rangeResult = await Session.Operations.SendAsync(
                    new GetTimeSeriesOperation<TTValues>(DocId, Name, from, to, start, pageSize, includes), Session._sessionInfo, token: token)
                .ConfigureAwait(false);

            if (rangeResult == null)
                return null;

            if (Session.NoTracking == false)
            {
                HandleIncludes(rangeResult);

                if (Session.TimeSeriesByDocId.TryGetValue(DocId, out var cache) == false)
                {
                    Session.TimeSeriesByDocId[DocId] = cache = new Dictionary<string, List<TimeSeriesRangeResult>>(StringComparer.OrdinalIgnoreCase);
                }

                if (cache.TryGetValue(Name, out var ranges) && ranges.Count > 0)
                {
                    // update
                    var index = ranges[0].From > to ? 0 : ranges.Count;
                    ranges.Insert(index, rangeResult);
                }

                else
                {
                    cache[Name] = new List<TimeSeriesRangeResult>
                    {
                        rangeResult
                    };
                }
            }

            return rangeResult.Entries;
        }

        private void HandleIncludes(TimeSeriesRangeResult rangeResult)
        {
            if (rangeResult.Includes == null) 
                return;

            using (rangeResult.Includes)
            {
                Session.RegisterIncludes(rangeResult.Includes);
            }

            rangeResult.Includes = null;
        }

        private static IEnumerable<TimeSeriesEntry> SkipAndTrimRangeIfNeeded(
            DateTime from,
            DateTime to,
            TimeSeriesRangeResult fromRange,
            TimeSeriesRangeResult toRange,
            List<TimeSeriesEntry> values,
            int skip,
            int trim)
        {
            if (fromRange != null && fromRange.To >= from)
            {
                // need to skip a part of the first range

                if (toRange != null && toRange.From <= to)
                {
                    // also need to trim a part of the last range
                    return values.Skip(skip).Take(values.Count - skip - trim);
                }

                return values.Skip(skip);
            }

            if (toRange != null && toRange.From <= to)
            {
                // trim a part of the last range
                return values.Take(values.Count - trim);
            }

            return values;
        }

        private async Task<IEnumerable<TimeSeriesEntry>>
            ServeFromCache(
                DateTime from,
                DateTime to,
                int start,
                int pageSize,
                Action<ITimeSeriesIncludeBuilder> includes,
                CancellationToken token)
        {
            var cache = Session.TimeSeriesByDocId[DocId];
            var ranges = cache[Name];

            // try to find a range in cache that contains [from, to]
            // if found, chop just the relevant part from it and return to the user.

            // otherwise, try to find two ranges (fromRange, toRange),
            // such that 'fromRange' is the last occurence for which range.From <= from
            // and 'toRange' is the first occurence for which range.To >= to.
            // At the same time, figure out the missing partial ranges that we need to get from the server.

            int toRangeIndex;
            var fromRangeIndex = -1;

            List<TimeSeriesRange> rangesToGetFromServer = default;

            for (toRangeIndex = 0; toRangeIndex < ranges.Count; toRangeIndex++)
            {
                if (ranges[toRangeIndex].From <= from)
                {
                    if ((ranges[toRangeIndex].To >= to) || (ranges[toRangeIndex].Entries.Length - start >= pageSize))
                    {
                        // we have the entire range in cache 
                        // we have all the range we need
                        // or that we have all the results we need in smaller range

                        return ChopRelevantRange(ranges[toRangeIndex], from, to, start, pageSize);
                    }

                    fromRangeIndex = toRangeIndex;
                    continue;
                }

                // can't get the entire range from cache

                rangesToGetFromServer ??= new List<TimeSeriesRange>();

                // add the missing part [f, t] between current range start (or 'from')
                // and previous range end (or 'to') to the list of ranges we need to get from server

                rangesToGetFromServer.Add(new TimeSeriesRange
                {
                    Name = Name,
                    From = toRangeIndex == 0 || ranges[toRangeIndex - 1].To < from
                        ? from
                        : ranges[toRangeIndex - 1].To,
                    To = ranges[toRangeIndex].From <= to
                        ? ranges[toRangeIndex].From
                        : to
                });

                if (ranges[toRangeIndex].To >= to)
                    break;
            }

            if (toRangeIndex == ranges.Count)
            {
                // requested range [from, to] ends after all ranges in cache
                // add the missing part between the last range end and 'to'
                // to the list of ranges we need to get from server

                rangesToGetFromServer ??= new List<TimeSeriesRange>();
                rangesToGetFromServer.Add(new TimeSeriesRange
                {
                    Name = Name,
                    From = ranges[ranges.Count - 1].To,
                    To = to
                });

            }

            // get all the missing parts from server

            Session.IncrementRequestCount();

            var details = await Session.Operations.SendAsync(
                    new GetMultipleTimeSeriesOperation(DocId, rangesToGetFromServer, start, pageSize, includes), Session._sessionInfo, token: token)
                .ConfigureAwait(false);

            if (includes != null)
            {
                RegisterIncludes(details);
            }

            // merge all the missing parts we got from server
            // with all the ranges in cache that are between 'fromRange' and 'toRange'

            var mergedValues = MergeRangesWithResults(from, to, ranges, fromRangeIndex, toRangeIndex,
                resultFromServer: details.Values[Name], out var resultToUser);

            if (Session.NoTracking == false)
            {
                from = details.Values[Name].Min(ts => ts.From);
                to = details.Values[Name].Max(ts => ts.To);
                InMemoryDocumentSessionOperations.AddToCache(Name, from, to, fromRangeIndex, toRangeIndex, ranges, cache, mergedValues);
            }

            return resultToUser;
        }

        private void RegisterIncludes(TimeSeriesDetails details)
        {
            Debug.Assert(details.Values[Name] != null, $"Invalid TimeSeriesDetails result : 'details.Values[{Name}]' is null");

            foreach (var rangeResult in details.Values[Name])
            {
                HandleIncludes(rangeResult);
            }
        }

        private static TimeSeriesEntry[] MergeRangesWithResults(DateTime from, DateTime to, List<TimeSeriesRangeResult> ranges,
            int fromRangeIndex,
            int toRangeIndex,
            List<TimeSeriesRangeResult> resultFromServer,
            out IEnumerable<TimeSeriesEntry> resultToUser)
        {
            var skip = 0;
            var trim = 0;
            var currentResultIndex = 0;
            var mergedValues = new List<TimeSeriesEntry>();

            var start = fromRangeIndex != -1 ? fromRangeIndex : 0;
            var end = toRangeIndex == ranges.Count ? ranges.Count - 1 : toRangeIndex;

            for (var i = start; i <= end; i++)
            {
                if (i == fromRangeIndex)
                {
                    if (ranges[i].From <= from && from <= ranges[i].To)
                    {
                        // requested range [from, to] starts inside 'fromRange'
                        // i.e fromRange.From <= from <= fromRange.To
                        // so we might need to skip a part of it when we return the
                        // result to the user (i.e. skip [fromRange.From, from])

                        if (ranges[i].Entries != null)
                        {
                            foreach (var v in ranges[i].Entries)
                            {
                                mergedValues.Add(v);
                                if (v.Timestamp < from)
                                {
                                    skip++;
                                }
                            }
                        }
                    }

                    continue;
                }

                if (currentResultIndex < resultFromServer.Count &&
                    resultFromServer[currentResultIndex].From < ranges[i].From)
                {
                    // add current result from server to the merged list
                    // in order to avoid duplication, skip first item in range
                    // (unless this is the first time we're adding to the merged list)

                    mergedValues.AddRange(resultFromServer[currentResultIndex++].Entries.Skip(mergedValues.Count == 0 ? 0 : 1));
                }

                if (i == toRangeIndex)
                {
                    if (ranges[i].From <= to)
                    {
                        // requested range [from, to] ends inside 'toRange'
                        // so we might need to trim a part of it when we return the
                        // result to the user (i.e. trim [to, toRange.to])

                        for (var index = mergedValues.Count == 0 ? 0 : 1; index < ranges[i].Entries.Length; index++)
                        {
                            mergedValues.Add(ranges[i].Entries[index]);
                            if (ranges[i].Entries[index].Timestamp > to)
                            {
                                trim++;
                            }
                        }
                    }

                    continue;
                }

                // add current range from cache to the merged list.
                // in order to avoid duplication, skip first item in range if needed

                mergedValues.AddRange(ranges[i].Entries.Skip(mergedValues.Count == 0 ? 0 : 1));
            }

            if (currentResultIndex < resultFromServer.Count)
            {
                // the requested range ends after all the ranges in cache,
                // so the last missing part is from server
                // add last missing part to the merged list

                mergedValues.AddRange(resultFromServer[currentResultIndex++].Entries.Skip(mergedValues.Count == 0 ? 0 : 1));
            }

            Debug.Assert(currentResultIndex == resultFromServer.Count);

            resultToUser = SkipAndTrimRangeIfNeeded(from, to,
                fromRange: fromRangeIndex == -1 ? null : ranges[fromRangeIndex],
                toRange: toRangeIndex == ranges.Count ? null : ranges[toRangeIndex],
                mergedValues, skip, trim);

            return mergedValues.ToArray();
        }

        private static IEnumerable<TimeSeriesEntry> ChopRelevantRange(TimeSeriesRangeResult range, DateTime from, DateTime to, int start, int pageSize)
        {
            if (range.Entries == null)
                yield break;

            foreach (var value in range.Entries)
            {
                if (value.Timestamp > to)
                    yield break;

                if (value.Timestamp < from)
                    continue;

                if (start-- > 0)
                    continue;

                if (pageSize-- <= 0)
                    yield break;

                yield return value;
            }
        }

        Task<TimeSeriesEntry<TValues>[]> IAsyncSessionDocumentTypedTimeSeries<TValues>.GetAsync(DateTime? from, DateTime? to, int start, int pageSize, CancellationToken token)
        {
            if (NotInCache(from, to))
            {
                // not in cache
                return GetTimeSeriesAndIncludes<TimeSeriesEntry<TValues>>(from, to, includes: null, start, pageSize, token);
            }

            return GetTypedFromCache<TValues>(from, to, includes: null, start, pageSize, token);
        }

        void ISessionDocumentTypedAppendTimeSeriesBase<TValues>.Append(DateTime timestamp, TValues entry, string tag)
        {
            Append(timestamp, entry, tag);
        }

        public void Append(TimeSeriesEntry<TValues> entry)
        {
            Append(entry.Timestamp, entry.Value, entry.Tag);
        }

        async Task<TimeSeriesRollupEntry<TValues>[]> IAsyncSessionDocumentRollupTypedTimeSeries<TValues>.GetAsync(DateTime? from, DateTime? to, int start, int pageSize, CancellationToken token)
        {
            if (NotInCache(from, to))
            {
                // not in cache
                return await GetTimeSeriesAndIncludes<TimeSeriesRollupEntry<TValues>>(from, to, includes: null, start, pageSize, token).ConfigureAwait(false);
            }

            var result = await GetTypedFromCache<TValues>(from, to, includes: null, start, pageSize, token).ConfigureAwait(false);
            return result?.Select(r => r.AsRollupEntry()).ToArray();
        }

        internal async Task<TimeSeriesStreamEnumerator<TTValues>> GetTimeSeriesStreamResult<TTValues>(DateTime? from = null, DateTime? to = null, TimeSpan? offset = null, CancellationToken token = default) where TTValues : TimeSeriesEntry
        {
            var streamOperation = new TimeSeriesStreamOperation(Session, DocId, Name, from, to, offset);
            var command = streamOperation.CreateRequest();
            await Session.RequestExecutor.ExecuteAsync(command, Session.Context, Session.SessionInfo, token).ConfigureAwait(false);
            var result = await streamOperation.SetResultAsync(command.Result, token).ConfigureAwait(false);
            return new TimeSeriesStreamEnumerator<TTValues>(result, token);
        }

        internal async Task<IAsyncEnumerator<TTValues>> GetAsyncStream<TTValues>(DateTime? from = null, DateTime? to = null, TimeSpan? offset = null, CancellationToken token = default) where TTValues : TimeSeriesEntry
        {
            return await GetTimeSeriesStreamResult<TTValues>(from, to, offset, token).ConfigureAwait(false);
        }

        internal async Task<IEnumerator<TTValues>> GetStream<TTValues>(DateTime? from = null, DateTime? to = null, TimeSpan? offset = null) where TTValues : TimeSeriesEntry
        {
            return await GetTimeSeriesStreamResult<TTValues>(from, to, offset).ConfigureAwait(false);
        }

        Task<IAsyncEnumerator<TimeSeriesEntry>> IAsyncTimeSeriesStreamingBase<TimeSeriesEntry>.StreamAsync(DateTime? from, DateTime? to, TimeSpan? offset, CancellationToken token)
        {
            return GetAsyncStream<TimeSeriesEntry>(from, to, offset, token);
        }

        Task<IAsyncEnumerator<TimeSeriesRollupEntry<TValues>>> IAsyncTimeSeriesStreamingBase<TimeSeriesRollupEntry<TValues>>.StreamAsync(DateTime? from, DateTime? to, TimeSpan? offset, CancellationToken token)
        {
            return GetAsyncStream<TimeSeriesRollupEntry<TValues>>(from, to, offset, token);
        }

        Task<IAsyncEnumerator<TimeSeriesEntry<TValues>>> IAsyncTimeSeriesStreamingBase<TimeSeriesEntry<TValues>>.StreamAsync(DateTime? from, DateTime? to, TimeSpan? offset, CancellationToken token)
        {
            return GetAsyncStream<TimeSeriesEntry<TValues>>(from, to, offset, token);
        }
    }
}
