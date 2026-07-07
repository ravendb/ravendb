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
    public sealed class AsyncSessionDocumentTimeSeries<TValues> : SessionTimeSeriesBase, IAsyncSessionDocumentTimeSeries, IAsyncSessionDocumentIncrementalTimeSeries, IAsyncSessionDocumentRollupTypedTimeSeries<TValues>, IAsyncSessionDocumentTypedTimeSeries<TValues>, IAsyncSessionDocumentTypedIncrementalTimeSeries<TValues> where TValues : new()
    {
        public AsyncSessionDocumentTimeSeries(InMemoryDocumentSessionOperations session, string documentId, string name) : base(session, documentId, name)
        {
        }

        public AsyncSessionDocumentTimeSeries(InMemoryDocumentSessionOperations session, object entity, string name) : base(session, entity, name)
        {
        }

        /// <inheritdoc cref="IAsyncSessionDocumentTimeSeries.GetAsync"/>
        public Task<TimeSeriesEntry[]> GetAsync(DateTime? from = null, DateTime? to = null, int start = 0, int pageSize = int.MaxValue, CancellationToken token = default)
        {
            return GetAsync(from, to, includes: null, start, pageSize, token);
        }

        /// <inheritdoc cref="IAsyncSessionDocumentTimeSeries.GetAsync(DateTime?, DateTime?, Action{ITimeSeriesIncludeBuilder}, int, int, CancellationToken)"/>
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

            if (resultToUser == null)
                return GetLocalEntriesForDeletedRange(from, to, pageSize)?.ToArray();

            resultToUser = OverlayLocalEntries(resultToUser, from ?? DateTime.MinValue, to ?? DateTime.MaxValue);

            return RemoveDeletedTimeSeries(resultToUser.Take(pageSize).ToList())?.ToArray();
        }

        private IEnumerable<TimeSeriesEntry> OverlayLocalEntries(IEnumerable<TimeSeriesEntry> serverEntries, DateTime from, DateTime to)
        {
            var local = GetCachedEntriesInRange<TimeSeriesEntry>(from, to);
            if (local == null || local.Count == 0)
                return serverEntries;

            return MergeSorted(serverEntries as List<TimeSeriesEntry> ?? serverEntries.ToList(), local);
        }

        private List<TTValues> GetCachedEntriesInRange<TTValues>(DateTime from, DateTime to)
            where TTValues : TimeSeriesEntry
        {
            if (Session.LocalTimeSeries.TryGetValue(DocId, out var byName) == false ||
                byName.TryGetValue(Name, out var entries) == false ||
                entries.Count == 0)
                return null;

            var result = new List<TTValues>();

            foreach (var kv in entries)
            {
                var e = kv.Value;
                if (e.Timestamp < from || e.Timestamp > to)
                    continue;

                var converted = ConvertCachedEntry<TTValues>(e);
                if (converted != null)
                    result.Add(converted);
            }

            return result.Count == 0 ? null : result;
        }

        private TTValues ConvertCachedEntry<TTValues>(TimeSeriesEntry entry) where TTValues : TimeSeriesEntry
        {
            if (entry is TTValues alreadyTyped)
                return alreadyTyped;

            var typed = new TimeSeriesEntry<TValues>
            {
                Timestamp = entry.Timestamp,
                Tag = entry.Tag,
                Values = entry.Values,
                IsRollup = entry.IsRollup,
                Value = TimeSeriesValuesHelper.SetMembers<TValues>(entry.Values, entry.IsRollup)
            };

            if (typed is TTValues typedResult)
                return typedResult;

            if (typeof(TTValues) == typeof(TimeSeriesRollupEntry<TValues>))
                return (TTValues)(object)typed.AsRollupEntry();

            return null;
        }

        internal List<TEntry> RemoveDeletedTimeSeries<TEntry>(List<TEntry> entries) where TEntry : TimeSeriesEntry
        {
            if (entries == null || entries.Count == 0)
                return entries;

            if (Session.DeletedTimeSeries.TryGetValue(DocId, out var cache) == false ||
                cache.TryGetValue(Name, out var ranges) == false ||
                ranges.Count == 0)
                return entries;

            var result = new List<TEntry>(entries.Count);

            foreach (var entry in entries)
            {
                bool deleted = false;

                foreach (var range in ranges)
                {
                    if (entry.Timestamp >= range.From && entry.Timestamp <= range.To)
                    {
                        deleted = true;
                        break;
                    }
                }

                if (deleted == false)
                    result.Add(entry);
            }

            return result;
        }

        // When ServeFromCache returns null the covering cached range is marked IsDeleted. In that case
        // we still surface any in-session appends made AFTER the delete (kept in the local overlay), so
        // they are not silently lost. Returns null when there are genuinely no such local entries, which
        // preserves the "deleted series, nothing appended -> null" contract for the callers.
        private List<TimeSeriesEntry> GetLocalEntriesForDeletedRange(DateTime? from, DateTime? to, int pageSize)
        {
            var localOnly = GetCachedEntriesInRange<TimeSeriesEntry>(from ?? DateTime.MinValue, to ?? DateTime.MaxValue);
            if (localOnly == null || localOnly.Count == 0)
                return null;

            return RemoveDeletedTimeSeries(localOnly.Take(pageSize).ToList());
        }

        internal async Task<TimeSeriesEntry<TEntry>[]> GetTypedFromCache<TEntry>(DateTime? from, DateTime? to, Action<ITimeSeriesIncludeBuilder> includes, int start,
            int pageSize, CancellationToken token = default) where TEntry : new()
        {
            // RavenDB-16060 
            // Typed TimeSeries results need special handling when served from cache
            // since we cache the results untyped 

            var resultToUser =
                await ServeFromCache(from ?? DateTime.MinValue, to ?? DateTime.MaxValue, start, pageSize, includes, token)
                    .ConfigureAwait(false);

            List<TimeSeriesEntry> asList;
            if (resultToUser == null)
            {
                asList = GetLocalEntriesForDeletedRange(from, to, pageSize);
                if (asList == null)
                    return null; // deleted series, nothing appended -> null (unchanged contract)
            }
            else
            {
                resultToUser = OverlayLocalEntries(resultToUser, from ?? DateTime.MinValue, to ?? DateTime.MaxValue);
                asList = RemoveDeletedTimeSeries(resultToUser.ToList());
            }

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
            from = (from ?? DateTime.MinValue).EnsureUtc();
            to = (to ?? DateTime.MaxValue).EnsureUtc();

            if (Session.TimeSeriesByDocId.TryGetValue(DocId, out var cache) &&
                cache.TryGetValue(Name, out var ranges))
            {
                foreach (var range in ranges)
                {
                    if (range.From <= to && range.To >= from)
                        return false;
                }
            }

            return true;
        }

        internal async Task<TTValues[]> GetTimeSeriesAndIncludes<TTValues>(DateTime? from, DateTime? to, Action<ITimeSeriesIncludeBuilder> includes, int start, int pageSize, CancellationToken token = default) where TTValues : TimeSeriesEntry
        {
            from = from?.EnsureUtc();
            to = to?.EnsureUtc();
            var cachedEntries = GetCachedEntriesInRange<TTValues>(from ?? DateTime.MinValue, to ?? DateTime.MaxValue);

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
                return (TTValues[])cachedEntries?.ToArray();

            var serverEntries = rangeResult.Entries ?? Array.Empty<TTValues>();
            var serverList = new List<TTValues>(serverEntries);

            var entriesResult = MergeSorted<TTValues>(serverList, cachedEntries);
            entriesResult = RemoveDeletedTimeSeries(entriesResult);

            if (Session.NoTracking == false)
            {
                HandleIncludes(rangeResult);

                if (Session.TimeSeriesByDocId.TryGetValue(DocId, out var cache) == false)
                {
                    Session.TimeSeriesByDocId[DocId] = cache = new Dictionary<string, List<TimeSeriesRangeResult>>(StringComparer.OrdinalIgnoreCase);
                }

                if (cache.TryGetValue(Name, out var ranges) && ranges.Count > 0)
                {
                    int index = ranges.Count;
                    while (index > 0 && ranges[index - 1].From > rangeResult.From)
                        index--;
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

            return entriesResult.ToArray();
        }

        // an incremental time series accumulates values; its in-session overlay stores only the
        // in-session delta, so the server base must be ADDED to it at read time (see CurrentLocalDeltaAt).
        private bool IsIncremental => Name.StartsWith(Constants.Headers.IncrementalTimeSeriesPrefix, StringComparison.OrdinalIgnoreCase);

        private List<TEntry> MergeSorted<TEntry>(List<TEntry> a, List<TEntry> b)
            where TEntry : TimeSeriesEntry
        {
            a ??= new();
            b ??= new();

            var result = new List<TEntry>(a.Count + b.Count);

            int i = 0, j = 0;

            while (i < a.Count && j < b.Count)
            {
                var ea = a[i];
                var eb = b[j];

                if (ea.Timestamp < eb.Timestamp)
                    result.Add(a[i++]);
                else if (eb.Timestamp < ea.Timestamp)
                    result.Add(b[j++]);
                else
                {
                    // same timestamp
                    if (IsIncremental && typeof(TEntry) == typeof(TimeSeriesEntry))
                    {
                        // incremental series: the local overlay holds the in-session delta only,
                        // so emit (server base + in-session delta). Build a new entry - a[i]/b[j] are
                        // shared with the cache/overlay and must not be mutated in place.
                        result.Add((TEntry)(object)new TimeSeriesEntry
                        {
                            Timestamp = eb.Timestamp,
                            Tag = eb.Tag,
                            IsRollup = eb.IsRollup,
                            Values = AddValues(ea.Values, eb.Values)
                        });
                    }
                    else
                    {
                        // append series: local (b) overrides the server value
                        result.Add(b[j]);
                    }

                    i++;
                    j++;
                }
            }

            while (i < a.Count)
                result.Add(a[i++]);
            while (j < b.Count)
                result.Add(b[j++]);

            return result;
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
                    if (((ranges[toRangeIndex].To >= to) || (ranges[toRangeIndex].CachedEntries.Count - start >= pageSize)))
                    {
                        if (ranges[toRangeIndex].IsDeleted == false)
                        {
                            // we have the entire range in cache 
                            // we have all the range we need
                            // or that we have all the results we need in smaller range

                            return ChopRelevantRange(ranges[toRangeIndex], from, to, start, pageSize);
                        }

                        return null;
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

        private static bool DuplicatesLastMerged(TimeSeriesEntry[] entries, List<TimeSeriesEntry> mergedValues)
        {
            return mergedValues.Count > 0 &&
                   entries != null &&
                   entries.Length > 0 &&
                   entries[0].Timestamp == mergedValues[mergedValues.Count - 1].Timestamp;
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
                    // skip its first item only if it duplicates the last merged entry
                    // (ranges don't always share a boundary entry)

                    var serverEntries = resultFromServer[currentResultIndex++].Entries;
                    mergedValues.AddRange(serverEntries.Skip(DuplicatesLastMerged(serverEntries, mergedValues) ? 1 : 0));
                }

                if (i == toRangeIndex)
                {
                    if (ranges[i].From <= to)
                    {
                        // requested range [from, to] ends inside 'toRange'
                        // so we might need to trim a part of it when we return the
                        // result to the user (i.e. trim [to, toRange.to])

                        var toEntries = ranges[i].Entries;
                        var index = DuplicatesLastMerged(toEntries, mergedValues) ? 1 : 0;
                        for (; index < toEntries.Length; index++)
                        {
                            mergedValues.Add(toEntries[index]);
                            if (toEntries[index].Timestamp > to)
                            {
                                trim++;
                            }
                        }
                    }

                    continue;
                }

                // add current range from cache to the merged list.
                // in order to avoid duplication, skip first item in range if needed

                mergedValues.AddRange(ranges[i].Entries.Skip(DuplicatesLastMerged(ranges[i].Entries, mergedValues) ? 1 : 0));
            }

            if (currentResultIndex < resultFromServer.Count)
            {
                // the requested range ends after all the ranges in cache,
                // so the last missing part is from server
                // add last missing part to the merged list

                var serverEntries = resultFromServer[currentResultIndex++].Entries;
                mergedValues.AddRange(serverEntries.Skip(DuplicatesLastMerged(serverEntries, mergedValues) ? 1 : 0));
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
            if (range.Entries == null || range.IsDeleted)
                yield break;

            foreach (var value in range.CachedEntries)
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

        private Task<TimeSeriesEntry<TValues>[]> GetAsyncInternal(DateTime? from, DateTime? to, int start, int pageSize, CancellationToken token)
        {
            if (NotInCache(from, to))
            {
                return GetTimeSeriesAndIncludes<TimeSeriesEntry<TValues>>(from, to, includes: null, start, pageSize, token);
            }

            return GetTypedFromCache<TValues>(from, to, includes: null, start, pageSize, token);
        }

        /// <inheritdoc cref="IAsyncSessionDocumentTypedTimeSeries{TValue}.GetAsync"/>
        Task<TimeSeriesEntry<TValues>[]> IAsyncSessionDocumentTypedTimeSeries<TValues>.GetAsync(DateTime? from, DateTime? to, int start, int pageSize, CancellationToken token)
        {
            return GetAsyncInternal(from, to, start, pageSize, token);
        }

        /// <inheritdoc cref="IAsyncSessionDocumentTypedIncrementalTimeSeries{TValue}.GetAsync"/>
        Task<TimeSeriesEntry<TValues>[]> IAsyncSessionDocumentTypedIncrementalTimeSeries<TValues>.GetAsync(DateTime? from, DateTime? to, int start, int pageSize, CancellationToken token)
        {
            return GetAsyncInternal(from, to, start, pageSize, token);
        }

        /// <inheritdoc cref="ISessionDocumentTypedAppendTimeSeriesBase{TValue}.Append"/>
        void ISessionDocumentTypedAppendTimeSeriesBase<TValues>.Append(DateTime timestamp, TValues entry, string tag)
        {
            Append(timestamp, entry, tag);
        }

        /// <inheritdoc cref="ISessionDocumentTypedAppendTimeSeriesBase{TValue}.Append(TimeSeriesEntry{TValue})"/>
        public void Append(TimeSeriesEntry<TValues> entry)
        {
            Append(entry.Timestamp, entry.Value, entry.Tag);
        }

        /// <inheritdoc cref="IAsyncSessionDocumentRollupTypedTimeSeries{TValue}.GetAsync"/>
        async Task<TimeSeriesRollupEntry<TValues>[]> IAsyncSessionDocumentRollupTypedTimeSeries<TValues>.GetAsync(DateTime? from, DateTime? to, int start, int pageSize, CancellationToken token)
        {
            if (NotInCache(from, to))
            {
                return await GetTimeSeriesAndIncludes<TimeSeriesRollupEntry<TValues>>(from, to, includes: null, start, pageSize, token)
                    .ConfigureAwait(false);
            }

            var result = await GetTypedFromCache<TValues>(from, to, includes: null, start, pageSize, token)
                .ConfigureAwait(false);
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

        /// <inheritdoc/>
        Task<IAsyncEnumerator<TimeSeriesEntry>> IAsyncTimeSeriesStreamingBase<TimeSeriesEntry>.StreamAsync(DateTime? from, DateTime? to, TimeSpan? offset, CancellationToken token)
        {
            return GetAsyncStream<TimeSeriesEntry>(from, to, offset, token);
        }

        /// <inheritdoc/>
        Task<IAsyncEnumerator<TimeSeriesRollupEntry<TValues>>> IAsyncTimeSeriesStreamingBase<TimeSeriesRollupEntry<TValues>>.StreamAsync(DateTime? from, DateTime? to, TimeSpan? offset, CancellationToken token)
        {
            return GetAsyncStream<TimeSeriesRollupEntry<TValues>>(from, to, offset, token);
        }

        /// <inheritdoc/>
        Task<IAsyncEnumerator<TimeSeriesEntry<TValues>>> IAsyncTimeSeriesStreamingBase<TimeSeriesEntry<TValues>>.StreamAsync(DateTime? from, DateTime? to, TimeSpan? offset, CancellationToken token)
        {
            return GetAsyncStream<TimeSeriesEntry<TValues>>(from, to, offset, token);
        }
    }
}
