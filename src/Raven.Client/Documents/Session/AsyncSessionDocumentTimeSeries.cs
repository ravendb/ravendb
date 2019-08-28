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

                // try to find a range in cache that contains [from, to]
                // if found, chop just the relevant part from it and return to user

                // otherwise, try to find two ranges (fromRange, toRange),
                // such that 'fromRange' is the last occurence for which range.From <= from
                // and 'toRange' is the first occurence for which range.To >= to.
                // At the same time, figure out the missing partial ranges that we need to get from server.

                var fromRangeIndex = -1;
                int toRangeIndex;

                var rangesToGetFromServer = new List<(DateTime From, DateTime To)>();

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

                    var (f, t) = (toRangeIndex == 0 || ranges[toRangeIndex - 1].To < from ? from : ranges[toRangeIndex - 1].To,
                        ranges[toRangeIndex].From <= to ? ranges[toRangeIndex].From : to);

                    rangesToGetFromServer.Add((f, t));

                    if (ranges[toRangeIndex].To >= to)
                        break;
                }

                // can't get the entire range from cache
                Session.IncrementRequestCount();

                if (toRangeIndex == ranges.Count)
                {
                    rangesToGetFromServer.Add((ranges[ranges.Count - 1].To, to));
                }

                details = await Session.Operations.SendAsync(
                        new GetTimeSeriesOperation(DocId, timeseries, rangesToGetFromServer), Session.SessionInfo, token: token)
                    .ConfigureAwait(false);

                var values = MergeRangesWithResult(
                    fromRangeIndex, 
                    toRangeIndex == ranges.Count ? ranges.Count - 1 : toRangeIndex,
                    details.Values[timeseries], 
                    out var skip, 
                    out var trim);

                if (fromRangeIndex == -1)
                {
                    // all ranges in cache start after 'from'

                    if (toRangeIndex == ranges.Count)
                    {
                        // the requested range [from, to] contains all the ranges that are in cache 

                        if (Session.NoTracking == false)
                        {
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
                        }

                        return values;

                    }

                    if (ranges[toRangeIndex].From > to)
                    {
                        // requested range ends before 'toRange'

                        if (Session.NoTracking == false)
                        {
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
                        }

                        return values;
                    }
                   
                    if (Session.NoTracking == false)
                    {
                        // merge the result from server into 'toRange'
                        // remove all ranges that come before 'toRange' from cache

                        ranges[toRangeIndex].From = from;
                        ranges[toRangeIndex].Values = values;
                        ranges.RemoveRange(0, toRangeIndex);
                    }

                    return values.Take(values.Length - trim);

                }
                
                if (toRangeIndex == ranges.Count)
                {
                    // all the ranges in cache end before 'to'

                    if (ranges[fromRangeIndex].To < from)
                    {
                        if (Session.NoTracking == false)
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
                        }

                        return values;
                    }

                    if (Session.NoTracking == false)
                    {
                        // merge result into 'fromRange'
                        // remove all the ranges from cache that come after 'fromRange' 

                        ranges[fromRangeIndex].To = to;
                        ranges[fromRangeIndex].Values = values;
                        ranges.RemoveRange(fromRangeIndex + 1, ranges.Count - fromRangeIndex - 1);
                    }

                    return values.Skip(skip);

                }

                // the requested range is inside cache bounds 

                if (ranges[fromRangeIndex].To < from)
                {
                    if (ranges[toRangeIndex].From > to)
                    {
                        if (Session.NoTracking == false)
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
                        }

                        return values;
                    }

                    if (Session.NoTracking == false)
                    {
                        // merge the new range into 'toRange'
                        // remove all ranges in between 'fromRange' and 'toRange'

                        ranges.RemoveRange(fromRangeIndex + 1, toRangeIndex - fromRangeIndex - 1);
                        ranges[toRangeIndex].From = from;
                        ranges[toRangeIndex].Values = values;
                    }

                    return values.Take(values.Length - trim);
                }

                if (ranges[toRangeIndex].From > to)
                {
                    if (Session.NoTracking == false)
                    {
                        // remove all ranges in between 'fromRange' and 'toRange'
                        // merge new range into 'fromRange'

                        ranges[fromRangeIndex].To = to;
                        ranges[fromRangeIndex].Values = values;
                        ranges.RemoveRange(fromRangeIndex + 1, toRangeIndex - fromRangeIndex - 1);
                    }

                    return values.Skip(skip);
                }

                if (Session.NoTracking == false)
                {
                    // merge all ranges in between 'fromRange' and 'toRange'
                    // into a single range [fromRange.From, toRange.To]

                    ranges[fromRangeIndex].To = ranges[toRangeIndex].To;
                    ranges[fromRangeIndex].Values = values;

                    ranges.RemoveRange(fromRangeIndex + 1, toRangeIndex - fromRangeIndex);
                }

                return values.Skip(skip).Take(values.Length - skip - trim);
                
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

            TimeSeriesValue[] MergeRangesWithResult(int fromRangeIndex, int toRangeIndex, List<TimeSeriesRange> resultFromServer, out int skip, out int trim)
            {
                skip = 0;
                trim = 0;
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
                                    skip++;
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
                                    trim++;
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

                return values.ToArray();
            }
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
