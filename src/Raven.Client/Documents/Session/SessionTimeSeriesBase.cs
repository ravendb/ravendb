using System;
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
            RemoveLocalEntries(from, to);

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
                    if (ranges[i].From >= from && ranges[i].To <= to)
                        ranges[i].IsDeleted = true;
                }

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
                Session.DeletedTimeSeries[DocId] = cache = new Dictionary<string, List<TimeSeriesRangeResult>>(StringComparer.OrdinalIgnoreCase);

            if (cache.TryGetValue(Name, out var ranges) == false)
                cache[Name] = ranges = new List<TimeSeriesRangeResult>();

            ranges.Add(range);
        }

        private void RemoveLocalEntries(DateTime? from, DateTime? to)
        {
            if (Session.LocalTimeSeries.TryGetValue(DocId, out var byName) == false ||
                byName.TryGetValue(Name, out var entries) == false ||
                entries.Count == 0)
                return;

            var f = (from ?? DateTime.MinValue).EnsureUtc();
            var t = (to ?? DateTime.MaxValue).EnsureUtc();

            int start = FindStartIndex(entries.Keys, f);
            int end = start;
            while (end < entries.Count && entries.Keys[end] <= t)
                end++;

            //full deletion, faster than for loop
            if (start == 0 && end == entries.Count)
            {
                entries.Clear();
                return;
            }

            for (int i = end - 1; i >= start; i--)
                entries.RemoveAt(i);
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
            var valuesArray = values.ToArray();

            if (increment)
                valuesArray = AddValues(CurrentLocalDeltaAt(utcTimestamp), valuesArray);

            // use the normalized timestamp: the entry is stored at utcTimestamp and the deleted ranges are
            // stored UTC/ms-normalized, so the hole must be punched at utcTimestamp too (RavenDB-25903, #4).
            RemoveFromDeletedCacheIfNeeded(utcTimestamp);

            var entry = new TimeSeriesEntry
            {
                Timestamp = utcTimestamp,
                Tag = tag,
                Values = valuesArray
            };

            if (Session.LocalTimeSeries.TryGetValue(DocId, out var byName) == false)
                Session.LocalTimeSeries[DocId] = byName = new Dictionary<string, SortedList<DateTime, TimeSeriesEntry>>(StringComparer.OrdinalIgnoreCase);

            if (byName.TryGetValue(Name, out var entries) == false)
                byName[Name] = entries = new SortedList<DateTime, TimeSeriesEntry>();

            entries[utcTimestamp] = entry;
        }

        // Returns only the in-session accumulated increment delta at the given timestamp (never the
        // cached server value). Increments store the pure in-session delta in the local overlay; the
        // server base is added at read time by the increment-aware merge (see MergeSorted). Reading the
        // cached server value here would bake the base into the overlay and lose it when the base is
        // unknown at increment time (e.g. incrementing before any read).
        private double[] CurrentLocalDeltaAt(DateTime utcTimestamp)
        {
            if (Session.LocalTimeSeries.TryGetValue(DocId, out var byName) &&
                byName.TryGetValue(Name, out var localEntries) &&
                localEntries.TryGetValue(utcTimestamp, out var local))
                return local.Values;

            return Array.Empty<double>();
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
                                ranges.RemoveAt(i--);
                            }
                            else
                            {
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

        protected static double[] AddValues(double[] existing, double[] delta)
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

        private static int FindStartIndex(IList<DateTime> keys, DateTime from)
        {
            int left = 0;
            int right = keys.Count - 1;
            int result = keys.Count;

            while (left <= right)
            {
                int mid = left + (right - left) / 2;

                if (keys[mid] >= from)
                {
                    result = mid;
                    right = mid - 1;
                }
                else
                {
                    left = mid + 1;
                }
            }

            return result;
        }
    }
}
