using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Primitives;
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

        public Dictionary<string, string[]> TimeSeriesToGetByDocId { get; }
        public Dictionary<string, List<TimeSeriesRangeResult>> Results { get; }

        public IncludeTimeSeriesCommand(DocumentDatabase database, DocumentsOperationContext context)
        {
            _database = database;
            _context = context;

            TimeSeriesToGetByDocId = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);
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

/*        public IncludeTimeSeriesCommand(DocumentDatabase database, DocumentsOperationContext context, Dictionary<string, HashSet<string>> countersBySourcePath)
            : this(database, context)
        {
            _countersBySourcePath = countersBySourcePath.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.ToArray());
        }*/

        public void Fill(Document document)
        {
            var docId = document.Id;

            foreach (var kvp in _timeSeriesBySourcePath)
            {
                if (kvp.Key != string.Empty &&
                    document.Data.TryGet(kvp.Key, out docId) == false)
                {
                    throw new InvalidOperationException($"Cannot include counters for related document '{kvp.Key}', " +
                                                        $"document {document.Id} doesn't have a field named '{kvp.Key}'. ");
                }

                if (Results.ContainsKey(docId))
                    continue;

                var timeSeriesToGet = kvp.Value.TimeseriesNames.ToArray();
                TimeSeriesToGetByDocId[docId] = timeSeriesToGet;

                var rangeResults = GetTimeSeriesForDocument(docId, kvp.Value);

                Results.Add(docId, rangeResults);
            }
        }

        private List<TimeSeriesRangeResult> GetTimeSeriesForDocument(LazyStringValue docId, IList<string> timeseriesNames, IList<string> fromList, IList<string> toList)
        {
            var rangeResults = new List<TimeSeriesRangeResult>();

            for (var i = 0; i < timeseriesNames.Count; i++)
            {
                var name = timeseriesNames[i];
                var (from, to) = TimeSeriesHandler.ParseDates(fromList[i], toList[i], name);

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

                var rangeResult = new TimeSeriesRangeResult
                {
                    Name = name,
                    From = from,
                    To = to,
                    Values = values.ToArray()
                };

                rangeResults.Add(rangeResult);
            }

            return rangeResults;
        }
    }
}
