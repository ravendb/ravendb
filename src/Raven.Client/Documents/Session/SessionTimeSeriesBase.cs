using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session.TimeSeries;
using Sparrow;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Abstract implementation for in memory session operations
    /// </summary>
    public abstract class SessionTimeSeriesBase
    {
        protected string DocId;
        protected string Name;
        protected InMemoryDocumentSessionOperations Session;

        protected SessionTimeSeriesBase(InMemoryDocumentSessionOperations session, string documentId, string name)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentNullException(nameof(documentId));

            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException(nameof(name));
            
            DocId = documentId;
            Name = name;
            Session = session;
        }

        protected SessionTimeSeriesBase(InMemoryDocumentSessionOperations session, object entity, string name)
        {
            if (entity == null)
                throw new ArgumentNullException(nameof(entity));

            if (session.DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false || document == null)
            {
                ThrowEntityNotInSession();
                return;
            }

            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException(nameof(name));

            DocId = document.Id;
            Name = name;
            Session = session;
        }

        /// <inheritdoc cref="ISessionDocumentAppendTimeSeriesBase.Append(DateTime, double, string)"/>
        public void Append(DateTime timestamp, double value, string tag = null)
        {
            Append(timestamp, new []{ value }, tag);
        }

        public void Append<TValues>(DateTime timestamp, TValues value, string tag = null)
        {
            if (value is IEnumerable<double> doubles)
            {
                Append(timestamp, doubles, tag);
                return;
            }

            var values = TimeSeriesValuesHelper.GetValues(value);
            Append(timestamp, values, tag);
        }

        /// <inheritdoc cref="ISessionDocumentAppendTimeSeriesBase.Append"/>
        public void Append(DateTime timestamp, IEnumerable<double> values, string tag = null)
        {
            if (Session.DocumentsById.TryGetValue(DocId, out DocumentInfo documentInfo) &&
                Session.DeletedEntities.Contains(documentInfo.Entity))
                ThrowDocumentAlreadyDeletedInSession(DocId, Name);

            var op = new TimeSeriesOperation.AppendOperation
            {
                Timestamp = timestamp.EnsureUtc(),
                Tag = tag,
                Values = values.ToArray()
            };

            if (Session.DeferredCommandsDictionary.TryGetValue((DocId, CommandType.TimeSeries, Name), out var command))
            {
                var tsCmd = (TimeSeriesBatchCommandData)command;
                tsCmd.TimeSeries.Append(op);
            }
            else
            {
                Session.Defer(new TimeSeriesBatchCommandData(DocId, Name, appends: new List<TimeSeriesOperation.AppendOperation> { op }, deletes: null));
            }

            if (Session.SessionInfo.NoCaching == false)
            {
                TrackTimeseriesInCache(timestamp, values, tag);
            }
        }

        /// <inheritdoc cref="ISessionDocumentDeleteTimeSeriesBase.Delete(DateTime)"/>
        public void Delete(DateTime at)
        {
            Delete(at, at);
        }

        /// <inheritdoc cref="ISessionDocumentDeleteTimeSeriesBase.Delete"/>
        public void Delete(DateTime? from = null, DateTime? to = null)
        {
            if (Session.DocumentsById.TryGetValue(DocId, out DocumentInfo documentInfo) &&
                Session.DeletedEntities.Contains(documentInfo.Entity))
                ThrowDocumentAlreadyDeletedInSession(DocId, Name);

            var op = new TimeSeriesOperation.DeleteOperation
            {
                From = from?.EnsureUtc(),
                To = to?.EnsureUtc()
            };

            if (Session.DeferredCommandsDictionary.TryGetValue((DocId, CommandType.TimeSeries, Name), out var command))
            {
                var tsCmd = (TimeSeriesBatchCommandData)command;
                tsCmd.TimeSeries.Delete(op);
            }
            else
            {
                Session.Defer(new TimeSeriesBatchCommandData(DocId, Name, appends: null, deletes: new List<TimeSeriesOperation.DeleteOperation> { op }));
            }

            RemoveFromCacheIfNeeded(from, to);
        }

        private void RemoveFromCacheIfNeeded(DateTime? from = null, DateTime? to = null)
        {
            if (Session.TimeSeriesByDocId.TryGetValue(DocId, out var cache) == false)
            {
                AddRemovedTimeSeriesRange(from, to);
                return;
            }
            
            if (from == null && to == null)
            {
                if (cache.TryGetValue(Name, out var allRanges))
                {
                    foreach (var range in allRanges)
                        range.IsDeleted = true;
                }
                AddRemovedTimeSeriesRange(from, to);
                return;
            }

            if (cache.TryGetValue(Name, out var ranges) && ranges.Count > 0)
            {
                from ??= DateTime.MinValue;
                to ??= DateTime.MaxValue;

                for (int i=0; i< ranges.Count; i++)
                {
                    var index = FindStartIndex(ranges[i].CachedEntries, from.Value);
                    if (ranges[i].From >= from && ranges[i].To <= to)
                        ranges[i].IsDeleted = true;
                    for (; index < ranges[i].CachedEntries.Count; index++)
                    {
                        if (ranges[i].CachedEntries[index].Timestamp >= from && ranges[i].CachedEntries[index].Timestamp <= to)
                        {
                            ranges[i].CachedEntries.RemoveAt(index--);
                        }
                    }

                    if (ranges[i].CachedEntries.Count == 0)
                    {
                        ranges.Remove(ranges[i--]);
                    }
                }

                //ranges.RemoveAll(range => range.From <= from && range.To >= to);
                AddRemovedTimeSeriesRange(from, to);
            }
        }

        private void AddRemovedTimeSeriesRange(DateTime? from = null, DateTime? to = null)
        {
            var range = new TimeSeriesRangeResult()
            {
                From = (from ?? DateTime.MinValue)
                    .EnsureUtc()
                    .EnsureMilliseconds(),
                To = (to ?? DateTime.MaxValue)
                    .EnsureUtc()
                    .EnsureMilliseconds()
            };
            if (Session.DeletedTimeSeries.TryGetValue(DocId, out var cache) == false)
            {
                cache = new Dictionary<string, List<TimeSeriesRangeResult>>();
                cache.Add(Name, new List<TimeSeriesRangeResult>());
                cache[Name].Add(range);
                Session.DeletedTimeSeries.Add(DocId, cache);
                return;
            }

            if (Session.DeletedTimeSeries.TryGetValue(DocId, out cache) && cache.TryGetValue(Name, out var ranges) == false)
            {
                cache.Add(Name, new List<TimeSeriesRangeResult>());
                cache[Name].Add(range);
                Session.DeletedTimeSeries[DocId] = cache;
                return;
            }
            Session.DeletedTimeSeries[DocId][Name].Add(range);
        }

        public void Increment<TValues>(DateTime timestamp, TValues value)
        {
            if (value is IEnumerable<double> doubles)
            {
                Increment(timestamp, doubles);
                return;
            }

            var values = TimeSeriesValuesHelper.GetValues(value);
            Increment(timestamp, values);
        }

        /// <inheritdoc cref="ISessionDocumentIncrementTimeSeriesBase.Increment"/>
        public void Increment(DateTime timestamp, IEnumerable<double> values)
        {
            if (Session.DocumentsById.TryGetValue(DocId, out DocumentInfo documentInfo) &&
                Session.DeletedEntities.Contains(documentInfo.Entity))
                ThrowDocumentAlreadyDeletedInSession(DocId, Name);

            var op = new TimeSeriesOperation.IncrementOperation()
            {
                Timestamp = timestamp.EnsureUtc(),
                Values = values is double[] doubleValues ? doubleValues : values.ToArray()
            };

            if (Session.DeferredCommandsDictionary.TryGetValue((DocId, CommandType.TimeSeriesWithIncrements, Name), out var command))
            {
                var tsCmd = (IncrementalTimeSeriesBatchCommandData)command;
                tsCmd.TimeSeries.Increment(op);
            }
            else
            {
                Session.Defer(new IncrementalTimeSeriesBatchCommandData(DocId, Name, increments: new List<TimeSeriesOperation.IncrementOperation> { op }));
            }

            // an increment accumulates onto whatever value is already known for this timestamp
            // (a server-loaded base and/or previous in-session increments), matching server semantics
            TrackTimeseriesInCache(timestamp, op.Values, increment: true);
        }

        /// <inheritdoc cref="ISessionDocumentIncrementTimeSeriesBase.Increment"/>
        public void Increment(IEnumerable<double> values)
        {
            Increment(DateTime.UtcNow, values);
        }

        /// <inheritdoc cref="ISessionDocumentIncrementTimeSeriesBase.Increment(DateTime, double)"/>
        public void Increment(DateTime timestamp, double value)
        {
            Increment(timestamp, new [] {value});
        }

        /// <inheritdoc cref="ISessionDocumentIncrementTimeSeriesBase.Increment(double)"/>
        public void Increment( double value)
        {
            Increment(DateTime.UtcNow, new [] { value });
        }

        private static void ThrowDocumentAlreadyDeletedInSession(string documentId, string timeseries)
        {
            throw new InvalidOperationException($"Can't modify timeseries {timeseries} of document {documentId}, the document was already deleted in this session.");
        }


        protected static void ThrowEntityNotInSession()
        {
            throw new ArgumentException("entity is not associated with the session, cannot perform timeseries operations on it. " +
                                        "Use documentId instead or track the entity in the session.");
        }

        private void TrackTimeseriesInCache(DateTime timestamp, IEnumerable<double> values, string tag = null, bool increment = false)
        {
            var utcTimestamp = timestamp.EnsureUtc().EnsureMilliseconds();
            var valuesArray = values as double[] ?? values.ToArray();

            //No timeseries were loaded for this document, we need to create the cache for it
            if (Session.TimeSeriesByDocId.TryGetValue(DocId, out var cache) == false)
            {
                cache = new Dictionary<string, List<TimeSeriesRangeResult>>(StringComparer.OrdinalIgnoreCase);
                Session.TimeSeriesByDocId[DocId] = cache;
            }

            RemoveFromDeletedCacheIfNeeded(timestamp);
            var earlier = Session.SessionCreatedAt < timestamp
                ? Session.SessionCreatedAt
                : timestamp;

            var latest = Session.SessionCreatedAt > timestamp
                ? Session.SessionCreatedAt
                : timestamp;

            //No timeseries with this name were loaded for this document, we need to create the cached range for it
            if (cache.TryGetValue(Name, out var ranges) == false)
            {
                ranges = new List<TimeSeriesRangeResult>();
                cache[Name] = ranges;

                var initialEntry = new TimeSeriesEntry
                {
                    Timestamp = utcTimestamp,
                    Tag = tag,
                    IsLocal = true,
                    Values = valuesArray
                };

                ranges.Add(new TimeSeriesRangeResult
                {
                    From = earlier,
                    To = latest,
                    IsLocal = true,
                    CachedEntries = new List<TimeSeriesEntry> { initialEntry },
                    Entries = new[] { initialEntry }
                });

                return;
            }

            //There are timeseries with this name for this document, we need to find the right range to add the new entry
            var tse = new TimeSeriesEntry
            {
                Timestamp = utcTimestamp,
                Tag = tag,
                Values = valuesArray
            };

            bool inserted = false;
            for (int i = ranges.Count - 1; i >= 0; i--)
            {
                if (ranges[i].From <= timestamp && ranges[i].To >= timestamp)
                {
                    inserted = true;
                    ranges[i].CachedEntries ??= ranges[i].Entries?.ToList() ?? new List<TimeSeriesEntry>();

                    int index = FindStartIndex(ranges[i].CachedEntries, tse.Timestamp);
                    var existsAtTimestamp = index < ranges[i].CachedEntries.Count &&
                                            ranges[i].CachedEntries[index].Timestamp == tse.Timestamp;

                    if (existsAtTimestamp)
                    {
                        // increment accumulates onto the existing value; a plain append overrides it
                        if (increment)
                            tse.Values = AddValues(ranges[i].CachedEntries[index].Values, valuesArray);

                        ranges[i].CachedEntries[index] = tse;
                    }
                    else
                    {
                        ranges[i].CachedEntries.Insert(index, tse);
                    }
                    break;
                }
            }

            if (inserted)
                return;

            //Timeseries entry is out of the range of the cached timeseries, we need to create a new range for it
            ranges.Add(new TimeSeriesRangeResult
            {
                From = earlier,
                To = latest,
                IsLocal = true,
                CachedEntries = new List<TimeSeriesEntry> { tse },
                Entries = new[] { tse }
            });
        }

        private void RemoveFromDeletedCacheIfNeeded(DateTime timestamp)
        {
            if (Session.DeletedTimeSeries.TryGetValue(DocId, out var cache))
            {
                if (cache.TryGetValue(Name, out var ranges))
                {
                    for (int i = 0; i < ranges.Count; i++)
                    {
                        if (timestamp >= ranges[i].From && timestamp <= ranges[i].To)
                        {

                            if (ranges[i].From == ranges[i].To)
                            {
                                //single tse range deletion
                                ranges.RemoveAt(i--);
                            }
                            else
                            {
                                //split the range by the timestamp
                                var newRange = new TimeSeriesRangeResult()
                                {
                                    To = ranges[i].To,
                                    From = timestamp.AddMilliseconds(1)
                                };

                                ranges[i].To = timestamp.AddMilliseconds(-1);
                                ranges.Insert(++i, newRange);
                            }
                        }
                    }
                }
            }
        }

        private static double[] AddValues(double[] existing, double[] delta)
        {
            existing ??= Array.Empty<double>();
            delta ??= Array.Empty<double>();

            var result = new double[Math.Max(existing.Length, delta.Length)];
            for (int i = 0; i < existing.Length; i++)
                result[i] = existing[i];
            for (int i = 0; i < delta.Length; i++)
                result[i] += delta[i];

            return result;
        }

        private int FindStartIndex(List<TimeSeriesEntry> entries, DateTime from)
        {
            int left = 0;
            int right = entries.Count - 1;
            int result = -1;

            while (left <= right)
            {
                int mid = left + (right - left) / 2;

                if (entries[mid].Timestamp >= from)
                {
                    result = mid;
                    right = mid - 1;
                }
                else
                {
                    left = mid + 1;
                }
            }

            return result == -1 ? entries.Count : result;
        }
    }
}
