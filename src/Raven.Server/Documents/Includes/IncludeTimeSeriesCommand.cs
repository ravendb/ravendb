using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Includes
{
    public class IncludeTimeSeriesCommand
    {
        private readonly DocumentDatabase _database;
        private readonly DocumentsOperationContext _context;
        private readonly Dictionary<string, HashSet<TimeSeriesRange>> _timeSeriesRangesBySourcePath;

        public Dictionary<string, List<TimeSeriesRangeResult>> Results { get; }

        public IncludeTimeSeriesCommand(DocumentDatabase database, DocumentsOperationContext context, Dictionary<string, HashSet<TimeSeriesRange>> timeSeriesRangesBySourcePath)
        {
            _database = database;
            _context = context;
            _timeSeriesRangesBySourcePath = timeSeriesRangesBySourcePath;

            Results = new Dictionary<string, List<TimeSeriesRangeResult>>(StringComparer.OrdinalIgnoreCase);
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

        private List<TimeSeriesRangeResult> GetTimeSeriesForDocument(string docId, HashSet<TimeSeriesRange> timeSeriesToGet)
        {
            var rangeResults = new List<TimeSeriesRangeResult>();

            foreach (var range in timeSeriesToGet)
            {
                var timeSeriesRangeResult = GetTimeSeries(docId, range.Name, range.From, range.To);
                rangeResults.Add(timeSeriesRangeResult);
            }

            return rangeResults;
        }

        private TimeSeriesRangeResult GetTimeSeries(string docId, string name, DateTime from, DateTime to)
        {
            var values = new List<TimeSeriesValue>();
            var reader = _database.DocumentsStorage.TimeSeriesStorage.GetReader(_context, docId, name, from, to);

            foreach (var singleResult in reader.AllValues())
            {
                values.Add(new TimeSeriesValue
                {
                    Timestamp = singleResult.TimeStamp,
                    Tag = singleResult.Tag,
                    Values = singleResult.Values.ToArray()
                });
            }

            return new TimeSeriesRangeResult
            {
                Name = name,
                From = from,
                To = to,
                Values = values.ToArray()
            };

        }
    }
}
