using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.Documents.Handlers;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Includes
{
    public class IncludeTimeSeriesCommand
    {
        private readonly DocumentsOperationContext _context;
        private readonly Dictionary<string, HashSet<TimeSeriesRange>> _timeSeriesRangesBySourcePath;

        public Dictionary<string, Dictionary<string, List<TimeSeriesRangeResult>>> Results { get; }

        public IncludeTimeSeriesCommand(DocumentsOperationContext context, Dictionary<string, HashSet<TimeSeriesRange>> timeSeriesRangesBySourcePath)
        {
            _context = context;
            _timeSeriesRangesBySourcePath = timeSeriesRangesBySourcePath;

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

        private Dictionary<string, List<TimeSeriesRangeResult>> GetTimeSeriesForDocument(string docId, HashSet<TimeSeriesRange> timeSeriesToGet)
        {
            var dictionary = new Dictionary<string, List<TimeSeriesRangeResult>>(StringComparer.OrdinalIgnoreCase);

            foreach (var range in timeSeriesToGet)
            {
                var start = 0;
                var pageSize = int.MaxValue;
                var timeSeriesRangeResult = TimeSeriesHandler.GetTimeSeriesRange(_context, docId, range.Name, range.From ?? DateTime.MinValue, range.To ?? DateTime.MaxValue, ref start, ref pageSize);
                if (timeSeriesRangeResult == null)
                {
                    Debug.Assert(pageSize <= 0, "Page size must be zero or less here");
                    return dictionary; 
                }

                if (dictionary.TryGetValue(range.Name, out var list) == false)
                {
                    dictionary[range.Name] = new List<TimeSeriesRangeResult>{ timeSeriesRangeResult };
                }
                else
                {
                    list.Add(timeSeriesRangeResult);
                }
            }

            return dictionary;
        }
    }
}
