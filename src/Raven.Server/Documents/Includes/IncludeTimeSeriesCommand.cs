using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session;
using Raven.Server.Documents.Handlers;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Includes
{
    public class IncludeTimeSeriesCommand
    {
        private readonly DocumentDatabase _database;
        private readonly DocumentsOperationContext _context;
        private readonly Dictionary<string, (IList<string> TimeseriesNames, IList<string> FromList, IList<string> ToList)> _timeSeriesBySourcePath;
        private readonly Dictionary<string, HashSet<TimeSeriesRange>> _timeSeriesRangesBySourcePath;


        public Dictionary<string, List<TimeSeriesRangeResult>> Results { get; }

        public IncludeTimeSeriesCommand(DocumentDatabase database, DocumentsOperationContext context)
        {
            _database = database;
            _context = context;

            Results = new Dictionary<string, List<TimeSeriesRangeResult>>(StringComparer.OrdinalIgnoreCase);
        }

        public IncludeTimeSeriesCommand(DocumentDatabase database, DocumentsOperationContext context, IList<string> timeseriesNames, IList<string> fromList, IList<string> toList) 
            : this(database, context)
        {
            _timeSeriesBySourcePath = new Dictionary<string, (IList<string>, IList<string>, IList<string>)>(StringComparer.OrdinalIgnoreCase)
            {
                [string.Empty] = (timeseriesNames, fromList, toList)
            };
        }

        public IncludeTimeSeriesCommand(DocumentDatabase database, DocumentsOperationContext context, Dictionary<string, HashSet<TimeSeriesRange>> timeSeriesRangesBySourcePath)
            : this(database, context)
        {
            _timeSeriesRangesBySourcePath = timeSeriesRangesBySourcePath;
        }

        public void Fill(Document document)
        {
            if (_timeSeriesRangesBySourcePath != null)
            {
                Fill(document, _timeSeriesRangesBySourcePath);
                return;
            }

            Fill(document, _timeSeriesBySourcePath);
        }


        private void Fill(Document document, dynamic timeSeriesToGet)
        {
            string docId = document.Id;

            foreach (var kvp in timeSeriesToGet)
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

        private List<TimeSeriesRangeResult> GetTimeSeriesForDocument(string docId, HashSet<TimeSeriesRange> timesSeriesToGet)
        {
            var rangeResults = new List<TimeSeriesRangeResult>();

            foreach (var range in timesSeriesToGet)
            {
                var timeSeriesRangeResult = GetTimeSeries(docId, range.Name, range.From, range.To);
                rangeResults.Add(timeSeriesRangeResult);
            }

            return rangeResults;
        }

        private List<TimeSeriesRangeResult> GetTimeSeriesForDocument(string docId, (IList<string> TimeseriesNames, IList<string> FromList, IList<string> ToList) timesSeriesToGet)
        {
            var rangeResults = new List<TimeSeriesRangeResult>();

            for (var i = 0; i < timesSeriesToGet.TimeseriesNames.Count; i++)
            {
                var name = timesSeriesToGet.TimeseriesNames[i];
                var (from, to) = TimeSeriesHandler.ParseDates(timesSeriesToGet.FromList[i], timesSeriesToGet.ToList[i], name);
                var timeSeriesRangeResult = GetTimeSeries(docId, name, from, to);
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
