using System;
using System.Collections.Generic;
using System.Diagnostics;
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

                var rangeResults = GetTimeSeriesForDocument(docId, kvp.Value);

                Results.Add(docId, rangeResults);
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
