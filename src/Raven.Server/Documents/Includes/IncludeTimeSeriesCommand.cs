using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.Documents.Handlers;
using Raven.Server.ServerWide.Context;
using Sparrow;

namespace Raven.Server.Documents.Includes
{
    public class IncludeTimeSeriesCommand
    {
        private readonly DocumentsOperationContext _context;
        private readonly Dictionary<string, HashSet<AbstractTimeSeriesRange>> _timeSeriesRangesBySourcePath;
        private readonly Dictionary<string, Dictionary<string, (long Count, DateTime Start, DateTime End)>> _timeSeriesStatsPerDocumentId;

        public Dictionary<string, Dictionary<string, List<TimeSeriesRangeResult>>> Results { get; }

        public IncludeTimeSeriesCommand(DocumentsOperationContext context, Dictionary<string, HashSet<AbstractTimeSeriesRange>> timeSeriesRangesBySourcePath)
        {
            _context = context;
            _timeSeriesRangesBySourcePath = timeSeriesRangesBySourcePath;

            _timeSeriesStatsPerDocumentId = new Dictionary<string, Dictionary<string, (long Count, DateTime Start, DateTime End)>>(StringComparer.OrdinalIgnoreCase);
            Results = new Dictionary<string, Dictionary<string, List<TimeSeriesRangeResult>>>(StringComparer.OrdinalIgnoreCase);
        }

        public void Fill(Document document)
        {
            if (document == null)
                return;

            string docId = document.Id;

            foreach (var kvp in _timeSeriesRangesBySourcePath)
            {
                if (kvp.Key != string.Empty &&
                    document.Data.TryGet(kvp.Key, out docId) == false)
                {
                    throw new InvalidOperationException($"Cannot include time series for related document '{kvp.Key}', " +
                                                        $"document {document.Id} doesn't have a field named '{kvp.Key}'. ");
                }

                if (Results.ContainsKey(docId))
                    continue;

                var count = kvp.Value.Count(x => x.Name == Constants.TimeSeries.All);
                switch (count)
                {
                    case 0:
                    {
                        var rangeResults = GetTimeSeriesForDocument(docId, kvp.Value);
                        Results.Add(docId, rangeResults);
                        break;
                    }
                    case 1:
                    {
                        // get all ts names
                        var timeSeriesNames = new List<string>();
                        TimeSeriesHandler.GetTimesSeriesNames(document, timeSeriesNames);
                        var arr = new HashSet<AbstractTimeSeriesRange>();
                        switch (kvp.Value.First())
                        {
                            case TimeSeriesRange r:
                                foreach (var name in timeSeriesNames)
                                    arr.Add(new TimeSeriesRange { Name = name, From = r.From, To = r.To });
                                break;
                            case TimeSeriesTimeRange tr:
                                foreach (var name in timeSeriesNames)
                                    arr.Add(new TimeSeriesTimeRange { Name = name, Time = tr.Time, Type = tr.Type });
                                break;
                            case TimeSeriesCountRange cr:
                                foreach (var name in timeSeriesNames)
                                    arr.Add(new TimeSeriesCountRange { Name = name, Count = cr.Count, Type = cr.Type });
                                break;
                            default:
                                throw new NotSupportedException($"Not supported time series range type '{kvp.Value.First()?.GetType().Name}'.");
                        }
                        var rangeResults = GetTimeSeriesForDocument(docId, arr);
                        Results.Add(docId, rangeResults);
                        break;
                    }
                    default:
                        throw new NotSupportedException($"Cannot have more than one include on '{Constants.TimeSeries.All}'.");
                }
            }
        }

        private Dictionary<string, List<TimeSeriesRangeResult>> GetTimeSeriesForDocument(string docId, HashSet<AbstractTimeSeriesRange> timeSeriesToGet)
        {
            var dictionary = new Dictionary<string, List<TimeSeriesRangeResult>>(StringComparer.OrdinalIgnoreCase);

            foreach (var range in timeSeriesToGet)
            {
                var start = 0;
                var pageSize = int.MaxValue;
                TimeSeriesRangeResult result;
                switch (range)
                {
                    case TimeSeriesRange r:
                        result = TimeSeriesHandler.GetTimeSeriesRange(_context, docId, r.Name, r.From ?? DateTime.MinValue, r.To ?? DateTime.MaxValue, ref start, ref pageSize);
                        if (result == null)
                        {
                            Debug.Assert(pageSize <= 0, "Page size must be zero or less here");
                            return dictionary;
                        }
                        break;
                    case TimeSeriesTimeRange tr:
                        {
                            var stats = GetTimeSeriesStats(docId, tr.Name);
                            if (stats.Count == 0)
                                continue;

                            DateTime from, to;
                            switch (tr.Type)
                            {
                                case TimeSeriesRangeType.Last:
                                    from = stats.End.Add(-tr.Time);
                                    to = DateTime.MaxValue;
                                    break;
                                default:
                                    throw new NotSupportedException($"Not supported time series range type '{tr.Type}'.");
                            }

                            result = TimeSeriesHandler.GetTimeSeriesRange(_context, docId, tr.Name, from, to, ref start, ref pageSize);
                        }
                        break;
                    case TimeSeriesCountRange cr:
                        {
                            var stats = GetTimeSeriesStats(docId, cr.Name);
                            if (stats.Count == 0)
                                continue;

                            if (stats.Count <= cr.Count)
                            {
                                switch (cr.Type)
                                {
                                    case TimeSeriesRangeType.Last:
                                        result = TimeSeriesHandler.GetTimeSeriesRange(_context, docId, cr.Name, stats.Start, DateTime.MaxValue, ref start, ref pageSize);
                                        break;
                                    default:
                                        throw new NotSupportedException($"Not supported time series range type '{cr.Type}'.");
                                }
                            }
                            else
                            {
                                switch (cr.Type)
                                {
                                    case TimeSeriesRangeType.Last:
                                        //TODO: what if start point is bigger than int max value
                                        var longStart = stats.Count - cr.Count;
                                        Debug.Assert(longStart < int.MaxValue, "longStart < int.MaxValue");
                                        var s = (int)longStart;
                                        result = TimeSeriesHandler.GetTimeSeriesRange(_context, docId, cr.Name, stats.Start, DateTime.MaxValue, ref s, ref pageSize);
                                        break;
                                    default:
                                        throw new NotSupportedException($"Not supported time series range type '{cr.Type}'.");
                                }
                            }
                        }
                        break;
                    default:
                        throw new NotSupportedException($"Not supported time series range type '{range?.GetType().Name}'.");
                }

                if (dictionary.TryGetValue(range.Name, out var list) == false)
                    dictionary[range.Name] = list = new List<TimeSeriesRangeResult>();

                list.Add(result);
            }

            return dictionary;

            (long Count, DateTime Start, DateTime End) GetTimeSeriesStats(string documentId, string timeSeries)
            {
                if (_timeSeriesStatsPerDocumentId.TryGetValue(documentId, out var timeSeriesStats) == false)
                    _timeSeriesStatsPerDocumentId[documentId] = timeSeriesStats = new Dictionary<string, (long Count, DateTime Start, DateTime End)>(StringComparer.OrdinalIgnoreCase);

                if (timeSeriesStats.TryGetValue(timeSeries, out var stats) == false)
                    timeSeriesStats[timeSeries] = stats = _context.DocumentDatabase.DocumentsStorage.TimeSeriesStorage.Stats.GetStats(_context, documentId, timeSeries);

                return stats;
            }
        }
    }
}
