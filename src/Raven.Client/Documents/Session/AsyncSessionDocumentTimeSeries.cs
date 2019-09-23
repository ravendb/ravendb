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
                cache.TryGetValue(timeseries, out var ranges) && 
                ranges.Count > 0)
            {
                if (ranges[0].From > to || ranges[ranges.Count - 1].To < from)
                {
                    // the entire range [from, to] is out of cache bounds

                    // e.g. if cache is : [[2,3], [4,6], [8,9]]
                    // and requested range is : [12, 15]
                    // then ranges[ranges.Count - 1].To < from 
                    // so we need to get [12,15] from server and place it
                    // at the end of the cache list

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

                var (servedFromCache, resultToUser, mergedValues, fromRangeIndex, toRangeIndex) = 
                    await ServeFromCacheOrGetMissingPartsFromServerAndMerge(timeseries, from, to, ranges, token)
                        .ConfigureAwait(false);

                if (servedFromCache == false && Session.NoTracking == false)
                {
                    AddToCache(timeseries, from, to, fromRangeIndex, toRangeIndex, ranges, cache, mergedValues);
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
                    Session.TimeSeriesByDocId[DocId] = cache = new Dictionary<string, List<TimeSeriesRangeResult>>(StringComparer.OrdinalIgnoreCase);
                }

                cache[timeseries] = new List<TimeSeriesRangeResult>
                {
                    details.Values[timeseries][0]
                };
            }

            return details.Values[timeseries][0].Values;
        }

        private static IEnumerable<TimeSeriesValue> SkipAndTrimRangeIfNeeded(
            DateTime from, 
            DateTime to, 
            TimeSeriesRangeResult fromRange, 
            TimeSeriesRangeResult toRange, 
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

        private async Task<(bool ServedFromCache, IEnumerable<TimeSeriesValue> ResultToUser, TimeSeriesValue[] MergedValues, int FromRangeIndex, int ToRangeIndex)>
            ServeFromCacheOrGetMissingPartsFromServerAndMerge(
                string timeseries,
                DateTime from,
                DateTime to,
                List<TimeSeriesRangeResult> ranges,
                CancellationToken token)
        {
            // try to find a range in cache that contains [from, to]
            // if found, chop just the relevant part from it and return to the user.

            // otherwise, try to find two ranges (fromRange, toRange),
            // such that 'fromRange' is the last occurence for which range.From <= from
            // and 'toRange' is the first occurence for which range.To >= to.
            // At the same time, figure out the missing partial ranges that we need to get from the server.

            int toRangeIndex;
            var fromRangeIndex = -1;

            List<TimeSeriesRange> rangesToGetFromServer = default;
            IEnumerable<TimeSeriesValue> resultToUser;

            for (toRangeIndex = 0; toRangeIndex < ranges.Count; toRangeIndex++)
            {
                if (ranges[toRangeIndex].From <= from)
                {
                    if (ranges[toRangeIndex].To >= to)
                    {
                        // we have the entire range in cache

                        resultToUser = ChopRelevantRange(ranges[toRangeIndex], from, to);
                        return (true, resultToUser, null, -1, -1);
                    }

                    fromRangeIndex = toRangeIndex;
                    continue;
                }

                // can't get the entire range from cache

                rangesToGetFromServer = rangesToGetFromServer ?? new List<TimeSeriesRange>();

                // add the missing part [f, t] between current range start (or 'from')
                // and previous range end (or 'to') to the list of ranges we need to get from server


                rangesToGetFromServer.Add(new TimeSeriesRange
                {
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

                rangesToGetFromServer = rangesToGetFromServer ?? new List<TimeSeriesRange>();
                rangesToGetFromServer.Add(new TimeSeriesRange
                {
                    From = ranges[ranges.Count - 1].To,
                    To = to
                });
            }

            // get all the missing parts from server

            Session.IncrementRequestCount();

            var details = await Session.Operations.SendAsync(
                    new GetTimeSeriesOperation(DocId, timeseries, rangesToGetFromServer), Session.SessionInfo, token: token)
                .ConfigureAwait(false);

            // merge all the missing parts we got from server
            // with all the ranges in cache that are between 'fromRange' and 'toRange'

            var mergedValues = MergeRangesWithResults(from, to, ranges, fromRangeIndex, toRangeIndex,
                resultFromServer: details.Values[timeseries], out resultToUser);

            return (false, resultToUser, mergedValues, fromRangeIndex, toRangeIndex);

        }

        private static TimeSeriesValue[] MergeRangesWithResults(DateTime @from, DateTime to, List<TimeSeriesRangeResult> ranges, int fromRangeIndex, int toRangeIndex, List<TimeSeriesRangeResult> resultFromServer, out IEnumerable<TimeSeriesValue> resultToUser)
        {
            var skip = 0;
            var trim = 0;
            var currentResultIndex = 0;
            var mergedValues = new List<TimeSeriesValue>();

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

                        foreach (var v in ranges[i].Values)
                        {
                            mergedValues.Add(v);
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
                    // add current result from server to the merged list
                    // in order to avoid duplication, skip first item in range
                    // (unless this is the first time we're adding to the merged list)

                    mergedValues.AddRange(resultFromServer[currentResultIndex++].Values.Skip(mergedValues.Count == 0 ? 0 : 1));
                }

                if (i == toRangeIndex)
                {
                    if (ranges[i].From <= to)
                    {
                        // requested range [from, to] ends inside 'toRange'
                        // so we might need to trim a part of it when we return the 
                        // result to the user (i.e. trim [to, toRange.to])

                        for (var index = mergedValues.Count == 0 ? 0 : 1; index < ranges[i].Values.Length; index++)
                        {
                            mergedValues.Add(ranges[i].Values[index]);
                            if (ranges[i].Values[index].Timestamp > to)
                            {
                                trim++;
                            }
                        }
                    }

                    continue;
                }

                // add current range from cache to the merged list.
                // in order to avoid duplication, skip first item in range if needed

                mergedValues.AddRange(ranges[i].Values.Skip(mergedValues.Count == 0 ? 0 : 1));
            }

            if (currentResultIndex < resultFromServer.Count)
            {
                // the requested range ends after all the ranges in cache,
                // so the last missing part is from server
                // add last missing part to the merged list

                mergedValues.AddRange(resultFromServer[currentResultIndex++].Values.Skip(mergedValues.Count == 0 ? 0 : 1));
            }

            Debug.Assert(currentResultIndex == resultFromServer.Count);

            resultToUser = SkipAndTrimRangeIfNeeded(from, to,
                fromRange: fromRangeIndex == -1 ? null : ranges[fromRangeIndex],
                toRange: toRangeIndex == ranges.Count ? null : ranges[toRangeIndex],
                mergedValues, skip, trim);

            return mergedValues.ToArray();
        }

        internal static void AddToCache(
            string timeseries, 
            DateTime from, 
            DateTime to, 
            int fromRangeIndex, 
            int toRangeIndex, 
            List<TimeSeriesRangeResult> ranges, 
            Dictionary<string, List<TimeSeriesRangeResult>> cache, 
            TimeSeriesValue[] values)
        {
            if (fromRangeIndex == -1)
            {
                // didn't find a 'fromRange' => all ranges in cache start after 'from'

                if (toRangeIndex == ranges.Count)
                {
                    // the requested range [from, to] contains all the ranges that are in cache 

                    // e.g. if cache is : [[2,3], [4,5], [7, 10]]
                    // and the requested range is : [1, 15]
                    // after this action cache will be : [[1, 15]]

                    cache[timeseries] = new List<TimeSeriesRangeResult>
                    {
                        new TimeSeriesRangeResult
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
                    // requested range ends before 'toRange' starts
                    // remove all ranges that come before 'toRange' from cache
                    // add the new range at the beginning of the list

                    // e.g. if cache is : [[2,3], [4,5], [7,10]]
                    // and the requested range is : [1,6]
                    // after this action cache will be : [[1,6], [7,10]]

                    ranges.RemoveRange(0, toRangeIndex);
                    ranges.Insert(0, new TimeSeriesRangeResult
                    {
                        Name = timeseries,
                        From = from,
                        To = to,
                        Values = values
                    });

                    return;
                }

                // the requested range ends inside 'toRange'
                // merge the result from server into 'toRange'
                // remove all ranges that come before 'toRange' from cache

                // e.g. if cache is : [[2,3], [4,5], [7,10]]
                // and the requested range is : [1,8]
                // after this action cache will be : [[1,10]]

                ranges[toRangeIndex].From = from;
                ranges[toRangeIndex].Values = values;
                ranges.RemoveRange(0, toRangeIndex);

                return;
            }

            // found a 'fromRange'

            if (toRangeIndex == ranges.Count)
            {
                // didn't find a 'toRange' => all the ranges in cache end before 'to'

                if (ranges[fromRangeIndex].To < from)
                {
                    // requested range starts after 'fromRange' ends,
                    // so it needs to be placed right after it
                    // remove all the ranges that come after 'fromRange' from cache 
                    // add the merged values as a new range at the end of the list

                    // e.g. if cache is : [[2,3], [5,6], [7,10]]
                    // and the requested range is : [4,12]
                    // then 'fromRange' is : [2,3]
                    // after this action cache will be : [[2,3], [4,12]]

                    ranges.RemoveRange(fromRangeIndex + 1, ranges.Count - fromRangeIndex - 1);
                    ranges.Add(new TimeSeriesRangeResult
                    {
                        From = from,
                        To = to,
                        Name = timeseries,
                        Values = values
                    });

                    return;
                }

                // the requested range starts inside 'fromRange'
                // merge result into 'fromRange'
                // remove all the ranges from cache that come after 'fromRange' 

                // e.g. if cache is : [[2,3], [4,6], [7,10]]
                // and the requested range is : [5,12]
                // then 'fromRange' is [4,6]
                // after this action cache will be : [[2,3], [4,12]]

                ranges[fromRangeIndex].To = to;
                ranges[fromRangeIndex].Values = values;
                ranges.RemoveRange(fromRangeIndex + 1, ranges.Count - fromRangeIndex - 1);

                return;
            }

            // found both 'fromRange' and 'toRange'
            // the requested range is inside cache bounds 

            if (ranges[fromRangeIndex].To < from)
            {
                // requested range starts after 'fromRange' ends

                if (ranges[toRangeIndex].From > to)
                {
                    // requested range ends before 'toRange' starts

                    // remove all ranges in between 'fromRange' and 'toRange'
                    // place new range in between 'fromRange' and 'toRange'

                    // e.g. if cache is : [[2,3], [5,6], [7,8], [10,12]]
                    // and the requested range is : [4,9]
                    // then 'fromRange' is [2,3] and 'toRange' is [10,12]
                    // after this action cache will be : [[2,3], [4,9], [10,12]]

                    ranges.RemoveRange(fromRangeIndex + 1, toRangeIndex - fromRangeIndex - 1);
                    ranges.Insert(fromRangeIndex + 1, new TimeSeriesRangeResult
                    {
                        Name = timeseries,
                        From = from,
                        To = to,
                        Values = values
                    });

                    return;
                }

                // requested range ends inside 'toRange' 

                // merge the new range into 'toRange'
                // remove all ranges in between 'fromRange' and 'toRange'

                // e.g. if cache is : [[2,3], [5,6], [7,10]]
                // and the requested range is : [4,9]
                // then 'fromRange' is [2,3] and 'toRange' is [7,10]
                // after this action cache will be : [[2,3], [4,10]]

                ranges.RemoveRange(fromRangeIndex + 1, toRangeIndex - fromRangeIndex - 1);
                ranges[toRangeIndex].From = from;
                ranges[toRangeIndex].Values = values;

                return;
            }

            // the requested range starts inside 'fromRange'

            if (ranges[toRangeIndex].From > to)
            {
                // requested range ends before 'toRange' starts

                // remove all ranges in between 'fromRange' and 'toRange'
                // merge new range into 'fromRange'

                // e.g. if cache is : [[2,4], [5,6], [8,10]]
                // and the requested range is : [3,7]
                // then 'fromRange' is [2,4] and 'toRange' is [8,10]
                // after this action cache will be : [[2,7], [8,10]]

                ranges[fromRangeIndex].To = to;
                ranges[fromRangeIndex].Values = values;
                ranges.RemoveRange(fromRangeIndex + 1, toRangeIndex - fromRangeIndex - 1);

                return;
            }

            // the requested range starts inside 'fromRange'
            // and ends inside 'toRange'

            // merge all ranges in between 'fromRange' and 'toRange'
            // into a single range [fromRange.From, toRange.To]

            // e.g. if cache is : [[2,4], [5,6], [8,10]]
            // and the requested range is : [3,9]
            // then 'fromRange' is [2,4] and 'toRange' is [8,10]
            // after this action cache will be : [[2,10]]

            ranges[fromRangeIndex].To = ranges[toRangeIndex].To;
            ranges[fromRangeIndex].Values = values;

            ranges.RemoveRange(fromRangeIndex + 1, toRangeIndex - fromRangeIndex);
        }

        private static IEnumerable<TimeSeriesValue> ChopRelevantRange(TimeSeriesRangeResult range, DateTime from, DateTime to)
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
