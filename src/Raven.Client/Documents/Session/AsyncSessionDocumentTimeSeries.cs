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
                if (ranges[0].From > to || ranges[ranges.Count - 1].To < from)
                {
                    // the entire range [from, to] is out of cache bounds

                    Session.IncrementRequestCount();

                    details = await Session.Operations.SendAsync(
                            new GetTimeSeriesOperation(DocId, timeseries, from, to), Session.SessionInfo, token: token)
                        .ConfigureAwait(false);

                    if (Session.NoTracking == false)
                    {
                        var index = ranges[0].From > to ? 0 : ranges.Count;
                        ranges.Insert(index, details.Values[timeseries][0]);
                    }

                    return details.Values[timeseries][0].Values;
                }

                if (GetFromCacheOrFigureOutMissingParts(from, to, ranges,
                    out var fromRangeIndex, out var toRangeIndex,
                    out var rangesToGetFromServer, out var resultToUser))
                {
                    return resultToUser;
                }

                // can't get the entire range from cache
                Session.IncrementRequestCount();

                details = await Session.Operations.SendAsync(
                        new GetTimeSeriesOperation(DocId, timeseries, rangesToGetFromServer), Session.SessionInfo, token: token)
                    .ConfigureAwait(false);

                var values = MergeRangesAndResults(
                    fromRangeIndex, 
                    toRangeIndex == ranges.Count ? ranges.Count - 1 : toRangeIndex,
                    details.Values[timeseries], 
                    out resultToUser);

                if (Session.NoTracking == false)
                {
                    AddToCache(timeseries, from, to, fromRangeIndex, toRangeIndex, ranges, cache, values);
                }

                return resultToUser;
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
                    details.Values[timeseries][0]
                };
                
            }

            return details.Values[timeseries][0].Values;

            TimeSeriesValue[] MergeRangesAndResults(int fromRangeIndex, int toRangeIndex, List<TimeSeriesRange> resultFromServer, out IEnumerable<TimeSeriesValue> resultToUser)
            {
                var skip = 0;
                var trim = 0;
                var currentResultIndex = 0;
                var values = new List<TimeSeriesValue>();

                var hasFromIndex = true;
                if (fromRangeIndex == -1)
                {
                    hasFromIndex = false;
                    fromRangeIndex = 0;
                }

                for (int i = fromRangeIndex; i <= toRangeIndex; i++)
                {                    
                    if (i == fromRangeIndex && hasFromIndex)
                    {
                        if (ranges[i].From <= from && ranges[i].To >= from)
                        {
                            foreach (var v in ranges[i].Values)
                            {
                                values.Add(v);
                                if (v.Timestamp < from)
                                {
                                    skip++;
                                }
                            }
                        }

                        continue;
                    }

                    if (currentResultIndex < resultFromServer.Count &&
                        resultFromServer[currentResultIndex].From < ranges[i].From)
                    {
                        values.AddRange(resultFromServer[currentResultIndex++].Values.Skip(values.Count == 0 ? 0 : 1));
                    }

                    if (i == toRangeIndex)
                    {
                        if (ranges[i].From <= to)
                        {
                            for (var index = values.Count == 0 ? 0 : 1; index < ranges[i].Values.Length; index++)
                            {
                                values.Add(ranges[i].Values[index]);
                                if (ranges[i].Values[index].Timestamp > to)
                                {
                                    trim++;
                                }
                            }
                        }

                        continue;
                    }

                    values.AddRange(ranges[i].Values.Skip(values.Count == 0 ? 0 : 1));
                }

                if (currentResultIndex < resultFromServer.Count)
                {
                    values.AddRange(resultFromServer[currentResultIndex++].Values.Skip(values.Count == 0 ? 0 : 1));
                }

                Debug.Assert(currentResultIndex == resultFromServer.Count);

                resultToUser = SkipAndTrimRangeIfNeeded(from, to, 
                    fromRange: fromRangeIndex == -1 ? null : ranges[fromRangeIndex], 
                    toRange: toRangeIndex == ranges.Count ? null : ranges[toRangeIndex], 
                    values, skip, trim);

                return values.ToArray();
            }
        }

        private static IEnumerable<TimeSeriesValue> SkipAndTrimRangeIfNeeded(
            DateTime from, 
            DateTime to, 
            TimeSeriesRange fromRange, 
            TimeSeriesRange toRange, 
            List<TimeSeriesValue> values, 
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

        private static bool GetFromCacheOrFigureOutMissingParts(
            DateTime from, 
            DateTime to,
            List<TimeSeriesRange> ranges, 
            out int fromRangeIndex, 
            out int toRangeIndex, 
            out List<(DateTime, DateTime)> rangesToGetFromServer,
            out IEnumerable<TimeSeriesValue> result)
        {
            // try to find a range in cache that contains [from, to]
            // if found, chop just the relevant part from it and return to the user.

            // otherwise, try to find two ranges (fromRange, toRange),
            // such that 'fromRange' is the last occurence for which range.From <= from
            // and 'toRange' is the first occurence for which range.To >= to.
            // At the same time, figure out the missing partial ranges that we need to get from the server.

            fromRangeIndex = -1;
            rangesToGetFromServer = default;

            for (toRangeIndex = 0; toRangeIndex < ranges.Count; toRangeIndex++)
            {
                if (ranges[toRangeIndex].From <= from)
                {
                    if (ranges[toRangeIndex].To >= to)
                    {
                        // we have the entire range in cache

                        result = ChopRelevantRange(ranges[toRangeIndex], from, to);
                        return true;                     
                    }

                    fromRangeIndex = toRangeIndex;
                    continue;
                }

                rangesToGetFromServer = rangesToGetFromServer ?? new List<(DateTime, DateTime)>();

                var (f, t) = (toRangeIndex == 0 || ranges[toRangeIndex - 1].To < from ? from : ranges[toRangeIndex - 1].To,
                    ranges[toRangeIndex].From <= to ? ranges[toRangeIndex].From : to);

                rangesToGetFromServer.Add((f, t));

                if (ranges[toRangeIndex].To >= to)
                    break;
            }

            if (toRangeIndex == ranges.Count)
            {
                rangesToGetFromServer = rangesToGetFromServer ?? new List<(DateTime, DateTime)>();
                rangesToGetFromServer.Add((ranges[ranges.Count - 1].To, to));
            }

            result = null;
            return false;
        }

        private static void AddToCache(
            string timeseries, 
            DateTime from, 
            DateTime to, 
            int fromRangeIndex, 
            int toRangeIndex, 
            List<TimeSeriesRange> ranges, 
            Dictionary<string, List<TimeSeriesRange>> cache, 
            TimeSeriesValue[] values)
        {
            if (fromRangeIndex == -1)
            {
                // all ranges in cache start after 'from'

                if (toRangeIndex == ranges.Count)
                {
                    // the requested range [from, to] contains all the ranges that are in cache 

                    cache[timeseries] = new List<TimeSeriesRange>
                    {
                        new TimeSeriesRange
                        {
                            Name = timeseries,
                            From = from,
                            To = to,
                            Values = values
                        }
                    };

                    return;
                }

                if (ranges[toRangeIndex].From > to)
                {
                    // requested range ends before 'toRange'
                    // remove all ranges that come before 'toRange' from cache
                    // add the new range at the beginning of the list

                    ranges.RemoveRange(0, toRangeIndex);
                    ranges.Insert(0, new TimeSeriesRange
                    {
                        Name = timeseries,
                        From = from,
                        To = to,
                        Values = values
                    });

                    return;
                }

                // merge the result from server into 'toRange'
                // remove all ranges that come before 'toRange' from cache

                ranges[toRangeIndex].From = from;
                ranges[toRangeIndex].Values = values;
                ranges.RemoveRange(0, toRangeIndex);

                return;
            }

            if (toRangeIndex == ranges.Count)
            {
                // all the ranges in cache end before 'to'

                if (ranges[fromRangeIndex].To < from)
                {
                    // remove all the ranges that come after 'fromRange' from cache 
                    // add the merged values as a new range at the end of the list

                    ranges.RemoveRange(fromRangeIndex + 1, ranges.Count - fromRangeIndex - 1);
                    ranges.Add(new TimeSeriesRange
                    {
                        From = from,
                        To = to,
                        Name = timeseries,
                        Values = values
                    });

                    return;
                }

                // merge result into 'fromRange'
                // remove all the ranges from cache that come after 'fromRange' 

                ranges[fromRangeIndex].To = to;
                ranges[fromRangeIndex].Values = values;
                ranges.RemoveRange(fromRangeIndex + 1, ranges.Count - fromRangeIndex - 1);

                return;
            }

            // the requested range is inside cache bounds 

            if (ranges[fromRangeIndex].To < from)
            {
                if (ranges[toRangeIndex].From > to)
                {
                    // remove all ranges in between 'fromRange' and 'toRange'
                    // place new range in between 'fromRange' and 'toRange'

                    ranges.RemoveRange(fromRangeIndex + 1, toRangeIndex - fromRangeIndex - 1);
                    ranges.Insert(fromRangeIndex + 1, new TimeSeriesRange
                    {
                        Name = timeseries,
                        From = from,
                        To = to,
                        Values = values
                    });

                    return;
                }

                // merge the new range into 'toRange'
                // remove all ranges in between 'fromRange' and 'toRange'

                ranges.RemoveRange(fromRangeIndex + 1, toRangeIndex - fromRangeIndex - 1);
                ranges[toRangeIndex].From = from;
                ranges[toRangeIndex].Values = values;

                return;
            }

            if (ranges[toRangeIndex].From > to)
            {
                // remove all ranges in between 'fromRange' and 'toRange'
                // merge new range into 'fromRange'

                ranges[fromRangeIndex].To = to;
                ranges[fromRangeIndex].Values = values;
                ranges.RemoveRange(fromRangeIndex + 1, toRangeIndex - fromRangeIndex - 1);

                return;
            }

            // merge all ranges in between 'fromRange' and 'toRange'
            // into a single range [fromRange.From, toRange.To]

            ranges[fromRangeIndex].To = ranges[toRangeIndex].To;
            ranges[fromRangeIndex].Values = values;

            ranges.RemoveRange(fromRangeIndex + 1, toRangeIndex - fromRangeIndex);
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
    }
}
