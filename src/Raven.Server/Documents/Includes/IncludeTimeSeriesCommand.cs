using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Server.ServerWide.Context;
using Sparrow.Server;

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

        private unsafe TimeSeriesRangeResult GetTimeSeries(string docId, string name, DateTime from, DateTime to)
        {
            var values = new List<TimeSeriesEntry>();
            var reader = _database.DocumentsStorage.TimeSeriesStorage.GetReader(_context, docId, name, from, to);

            // init hash 
            var size = Sodium.crypto_generichash_bytes();
            Debug.Assert((int)size == 32);
            var cryptoGenerichashStatebytes = (int)Sodium.crypto_generichash_statebytes();
            var state = stackalloc byte[cryptoGenerichashStatebytes];
            if (Sodium.crypto_generichash_init(state, null, UIntPtr.Zero, size) != 0)
                ComputeHttpEtags.ThrowFailToInitHash();
            
            foreach (var (individualValues, segmentResult) in reader.SegmentsOrValues())
            {
                var enumerable = individualValues ?? segmentResult.Values;

                foreach (var singleResult in enumerable)
                {
                    values.Add(new TimeSeriesEntry
                    {
                        Timestamp = singleResult.Timestamp,
                        Tag = singleResult.Tag,
                        Values = singleResult.Values.ToArray()
                    });
                }

                ComputeHttpEtags.HashChangeVector(state, segmentResult?.ChangeVector);
            }

            return new TimeSeriesRangeResult
            {
                Name = name,
                From = from,
                To = to,
                Entries = values.ToArray(),
                Hash = ComputeHttpEtags.FinalizeHash(size, state)
            };

        }
    }
}
