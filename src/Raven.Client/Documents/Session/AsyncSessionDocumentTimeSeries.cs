//-----------------------------------------------------------------------
// <copyright file="AsyncSessionDocumentTimeSeries.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.TimeSeries;
using Sparrow.Json;

namespace Raven.Client.Documents.Session
{
    public class AsyncSessionDocumentTimeSeries : SessionTimeSeriesBase, IAsyncSessionDocumentTimeSeries
    {
        public AsyncSessionDocumentTimeSeries(InMemoryDocumentSessionOperations session, string documentId) : base(session, documentId)
        {
        }

        public AsyncSessionDocumentTimeSeries(InMemoryDocumentSessionOperations session, object entity) : base(session, entity)
        {
        }
        public async Task<IEnumerable<TimeSeriesValue>> GetAsync(string timeseries, DateTime from, DateTime to, CancellationToken token = default)
        {
            TimeSeriesDetails details;

            if (Session.TimeSeriesByDocId.TryGetValue(DocId, out var cache) &&
                cache.TryGetValue(timeseries, out var ranges))
            {
                if (ranges[0].FullRange)
                {
                    // TODO support this

                    // we have all values, chop relevant part
                    return ChopRelevantRange(ranges[0], from, to);
                }

                if (ranges[0].From > to || ranges[ranges.Count - 1].To < from)
                {
                    // the requested range is out of cache bounds
                    // get entire range [from, to] from server

                    Session.IncrementRequestCount();

                    details = await Session.Operations.SendAsync(
                            new GetTimeSeriesOperation(DocId, timeseries, from, to), Session.SessionInfo, token: token)
                        .ConfigureAwait(false);

                    if (Session.NoTracking == false)
                    {
                        var index = ranges[0].From > to ? 0 : ranges.Count;
                        ranges.Insert(index, details.Values[timeseries]);
                    }

                    return details.Values[timeseries].Values;
                }

                // try to find a range in cache that contains [from, to]
                // if found, chop just the relevant part from it and return to user

                // otherwise, try to find two ranges (fromRange, toRange),
                // such that 'fromRange' is the last occurence for which range.From <= from
                // and 'toRange' is the first occurence for which range.To >= to.

                var fromRangeIndex = -1;
                int toRangeIndex;

                for (toRangeIndex = 0; toRangeIndex < ranges.Count; toRangeIndex++)
                {
                    if (ranges[toRangeIndex].From <= from)
                    {
                        if (ranges[toRangeIndex].To >= to)
                        {
                            // we have the entire range in cache
                            return ChopRelevantRange(ranges[toRangeIndex], from, to);
                        }

                        fromRangeIndex = toRangeIndex;
                        continue;
                    }

                    if (ranges[toRangeIndex].To >= to)
                        break;
                }

                // can't get the entire range from cache
                Session.IncrementRequestCount();

                if (fromRangeIndex == -1)
                {
                    // all ranges in cache start after 'from'

                    if (toRangeIndex == ranges.Count)
                    {
                        // the requested range [from, to] contains all the ranges that are in cache 

                        details = await Session.Operations.SendAsync(
                                new GetTimeSeriesOperation(DocId, timeseries, from, to), Session.SessionInfo, token: token)
                            .ConfigureAwait(false);

                        if (Session.NoTracking == false)
                        {
                            cache[timeseries] = new List<TimeSeriesRange>
                            {
                                new TimeSeriesRange
                                {
                                    Name = timeseries,
                                    From = from,
                                    To = to,
                                    FullRange = false,
                                    Values = details.Values[timeseries].Values
                                }
                            };
                        }

                        return details.Values[timeseries].Values;
                    }

                    if (ranges[toRangeIndex].From >= to)
                    {
                        // get entire range [from, to] from server
                        // remove all ranges that come before 'toRange' from cache
                        // add the new range at the beginning of the list

                        details = await Session.Operations.SendAsync(
                                new GetTimeSeriesOperation(DocId, timeseries, from, to), Session.SessionInfo, token: token)
                            .ConfigureAwait(false);

                        if (Session.NoTracking == false)
                        {
                            ranges.RemoveRange(0, toRangeIndex);
                            ranges.Insert(0, new TimeSeriesRange
                            {
                                Name = timeseries,
                                From = from,
                                To = to,
                                FullRange = false,
                                Values = details.Values[timeseries].Values
                            });
                        }

                        return details.Values[timeseries].Values;
                    }

                    // get partial range [from, toRange.From] from server
                    // merge the new range into 'toRange'
                    // remove all ranges that come before 'toRange' from cache

                    details = await Session.Operations.SendAsync(
                            new GetTimeSeriesOperation(DocId, timeseries, from, ranges[toRangeIndex].From), Session.SessionInfo, token: token)
                        .ConfigureAwait(false);

                    var newValues = MergeRanges(details.Values[timeseries].Values, ranges[toRangeIndex].Values, from, to, out _, out var take);

                    if (Session.NoTracking == false)
                    {
                        ranges[toRangeIndex].From = from;
                        ranges[toRangeIndex].Values = newValues;
                        ranges.RemoveRange(0, toRangeIndex);
                    }

                    return newValues.Take(take);
                }

                if (toRangeIndex == ranges.Count)
                {
                    // found a matching 'fromRange'
                    // all the ranges in cache end before 'to'

                    if (ranges[fromRangeIndex].To < from)
                    {
                        // get entire range [from, to] from server
                        // remove all the ranges that come after 'fromRange' from cache 
                        // add new range to the end of the list

                        details = await Session.Operations.SendAsync(
                                new GetTimeSeriesOperation(DocId, timeseries, from, to), Session.SessionInfo, token: token)
                            .ConfigureAwait(false);

                        if (Session.NoTracking == false)
                        {
                            ranges.RemoveRange(fromRangeIndex + 1, ranges.Count - fromRangeIndex -1);

                            ranges.Add(new TimeSeriesRange
                            {
                                From = from,
                                To = to,
                                FullRange = false,
                                Name = timeseries,
                                Values = details.Values[timeseries].Values
                            });
                        }

                        return details.Values[timeseries].Values;
                    }

                    // get partial range [fromRange.To, to] from server
                    // merge it into 'fromRange'
                    // remove all the ranges from cache that come after 'fromRange' 

                    details = await Session.Operations.SendAsync(
                            new GetTimeSeriesOperation(DocId, timeseries, ranges[fromRangeIndex].To, to), Session.SessionInfo, token: token)
                        .ConfigureAwait(false);

                    var newValues = MergeRanges(ranges[fromRangeIndex].Values, details.Values[timeseries].Values, from, to, out var toSkip, out _);

                    if (Session.NoTracking == false)
                    {
                        ranges[fromRangeIndex].To = to;
                        ranges[fromRangeIndex].Values = newValues;
                        ranges.RemoveRange(fromRangeIndex + 1, ranges.Count - fromRangeIndex - 1);
                    }

                    return newValues.Skip(toSkip);
                }

                if (ranges[fromRangeIndex].To < from)
                {
                    if (ranges[toRangeIndex].From > to)
                    {
                        // get entire range [from, to] from server
                        // remove all ranges in between fromRange and toRange
                        // place new range in between fromRange and toRange

                        details = await Session.Operations.SendAsync(
                                new GetTimeSeriesOperation(DocId, timeseries, from, to), Session.SessionInfo, token: token)
                            .ConfigureAwait(false);

                        if (Session.NoTracking == false)
                        {
                            ranges.RemoveRange(fromRangeIndex + 1, toRangeIndex - fromRangeIndex - 1);
                            ranges.Insert(fromRangeIndex + 1, new TimeSeriesRange
                            {
                                Name = timeseries,
                                From = from,
                                To = to,
                                Values = details.Values[timeseries].Values
                            });
                        }

                        return details.Values[timeseries].Values;
                    }

                    // get range [from, toRange.From] from server
                    // merge the new range into 'toRange'
                    // remove all ranges in between 'fromRange' and 'toRange'

                    details = await Session.Operations.SendAsync(
                            new GetTimeSeriesOperation(DocId, timeseries, from, ranges[toRangeIndex].From), Session.SessionInfo, token: token)
                        .ConfigureAwait(false);

                    var newValues = MergeRanges(details.Values[timeseries].Values, ranges[toRangeIndex].Values, from, to, out _, out var take);

                    if (Session.NoTracking == false)
                    {
                        ranges.RemoveRange(fromRangeIndex + 1, toRangeIndex - fromRangeIndex - 1);
                        ranges[toRangeIndex].From = from;
                        ranges[toRangeIndex].Values = newValues;
                    }

                    return newValues.Take(take);
                }

                if (ranges[toRangeIndex].From > to)
                {
                    // get partial range [fromRange.To, to] from server
                    // remove all ranges in between 'fromRange' and 'toRange'
                    // merge new range into 'fromRange'

                    details = await Session.Operations.SendAsync(
                            new GetTimeSeriesOperation(DocId, timeseries, ranges[fromRangeIndex].To, to), Session.SessionInfo, token: token)
                        .ConfigureAwait(false);

                    var newValues = MergeRanges(ranges[fromRangeIndex].Values, details.Values[timeseries].Values, from, to, out var toSkip, out _);

                    if (Session.NoTracking == false)
                    {
                        ranges[fromRangeIndex].To = to;
                        ranges[fromRangeIndex].Values = newValues;
                        ranges.RemoveRange(fromRangeIndex + 1, toRangeIndex - fromRangeIndex - 1);
                    }

                    return newValues.Skip(toSkip);
                }

                // get range [fromRange.To, toRange.From] from server and merge all 
                // ranges in between 'fromRange' and 'toRange' into a single range [fromRange.From, toRange.To]

                details = await Session.Operations.SendAsync(
                        new GetTimeSeriesOperation(DocId, timeseries, ranges[fromRangeIndex].To, ranges[toRangeIndex].From), Session.SessionInfo, token: token)
                    .ConfigureAwait(false);

                var firstMergeSize = (ranges[fromRangeIndex].Values.Length == 0 ? 0 : ranges[fromRangeIndex].Values.Length - 1) + details.Values[timeseries].Values.Length;
                var size = firstMergeSize + (ranges[toRangeIndex].Values.Length == 0 ? 0 : ranges[toRangeIndex].Values.Length - 1);
                var values = MergeRanges(ranges[fromRangeIndex].Values, details.Values[timeseries].Values, from, to, out var skip, out _, size);

                var toTake = firstMergeSize - skip;
                var offset = firstMergeSize == 0 ? 0 : firstMergeSize - 1;

                for (var i = firstMergeSize == 0 ? 0 : 1; i < ranges[toRangeIndex].Values.Length; i++)
                {
                    var current = ranges[toRangeIndex].Values[i];
                    if (current.Timestamp <= to)
                    {
                        toTake++;
                    }

                    values[i + offset] = current;
                }

                if (Session.NoTracking == false)
                {
                    ranges[fromRangeIndex].To = ranges[toRangeIndex].To;
                    ranges[fromRangeIndex].Values = values;

                    ranges.RemoveRange(fromRangeIndex + 1, toRangeIndex - fromRangeIndex);
                }

                return values.Skip(skip).Take(toTake);
            }

            if (Session.DocumentsById.TryGetValue(DocId, out var document) &&
                document.Metadata.TryGet(Constants.Documents.Metadata.TimeSeries, out BlittableJsonReaderArray metadataTimeSeries) &&
                metadataTimeSeries.BinarySearch(timeseries) < 0)
            {
                // the document is loaded in the session, but the metadata says that there is no such timeseries
                return Array.Empty<TimeSeriesValue>();
            }

            Session.IncrementRequestCount();

            details = await Session.Operations.SendAsync(
                    new GetTimeSeriesOperation(DocId, timeseries, from, to), Session.SessionInfo, token: token)
                .ConfigureAwait(false);

            if (Session.NoTracking == false)
            {
                if (Session.TimeSeriesByDocId.TryGetValue(DocId, out cache) == false)  
                {
                    Session.TimeSeriesByDocId[DocId] = cache = new Dictionary<string, List<TimeSeriesRange>>(StringComparer.OrdinalIgnoreCase);
                }

                cache[timeseries] = new List<TimeSeriesRange>
                {
                    details.Values[timeseries]
                };
                
            }

            return details.Values[timeseries].Values;

        }

        private static IEnumerable<TimeSeriesValue> ChopRelevantRange(TimeSeriesRange range, DateTime from, DateTime to)
        {
            foreach (var value in range.Values)
            {
                if (value.Timestamp > to)
                    yield break;

                if (value.Timestamp < from)
                    continue;

                yield return value;
            }
        }

        private static TimeSeriesValue[] MergeRanges(TimeSeriesValue[] range1, TimeSeriesValue[] range2, DateTime from, DateTime to, out int skip, out int take, int? size = null)
        {
            skip = 0;
            var newValues = new TimeSeriesValue[size ?? (range1.Length == 0 ? 0 : range1.Length - 1) + range2.Length];
            
            for (var i = 0; i < range1.Length; i++)
            {
                var current = range1[i];
                if (current.Timestamp < from)
                {
                    skip++;
                }

                newValues[i] = current;
            }

            take = range1.Length;
            var offset = range1.Length == 0 ? 0 : range1.Length - 1;

            for (var i = range1.Length == 0 ? 0 : 1; i < range2.Length; i++)
            {
                var current = range2[i];
                if (current.Timestamp <= to)
                {
                    take++;
                }
                newValues[i + offset] = current;
            }

            return newValues;

        }
    }
}
