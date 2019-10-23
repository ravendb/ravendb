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
        private readonly Dictionary<string, (IList<string> TimeSeriesNames, IList<string> FromList, IList<string> ToList)> _timeSeriesBySourcePath;
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
            if (timeseriesNames?.Count != fromList.Count || fromList.Count != toList.Count)
                throw new InvalidOperationException("Parameters 'timeseriesNames', 'fromList' and 'toList' must be of equal length. " +
                                                    $"Got : timeseriesNames.Count = {timeseriesNames?.Count}, fromList.Count = {fromList?.Count}, toList.Count = {toList?.Count}.");

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

        private List<TimeSeriesRangeResult> GetTimeSeriesForDocument(string docId, (IList<string> TimeSeriesNames, IList<string> FromList, IList<string> ToList) timesSeriesToGet)
        {
            var stringsToDates = new Dictionary<string, DateTime>();

            var rangeResults = new List<TimeSeriesRangeResult>();

            for (var i = 0; i < timesSeriesToGet.TimeSeriesNames.Count; i++)
            {
                var name = timesSeriesToGet.TimeSeriesNames[i];
                var fromStr = timesSeriesToGet.FromList[i];
                var toStr = timesSeriesToGet.ToList[i];

                if (stringsToDates.TryGetValue(fromStr, out var from) == false)
                {
                    stringsToDates[fromStr] = from = TimeSeriesHandler.ParseDate(fromStr, name);
                }
                if (stringsToDates.TryGetValue(toStr, out var to) == false)
                {
                    stringsToDates[toStr] = to = TimeSeriesHandler.ParseDate(toStr, name);
                }

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
