using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session.Loaders;
using Raven.Server.Documents.Handlers;


namespace Raven.Server.Documents.Queries.Counters
{
    public class TimeSeriesIncludesField
    {
        public TimeSeriesIncludesField()
        {
            TimeSeries = new Dictionary<string, HashSet<TimeSeriesRange>>(StringComparer.OrdinalIgnoreCase);
        }

        public readonly Dictionary<string, HashSet<TimeSeriesRange>> TimeSeries;

        public void AddTimeSeries(string timeseries, string fromStr, string toStr, string sourcePath = null)
        {
            var key = sourcePath ?? string.Empty;
            if (TimeSeries.TryGetValue(key, out var hashSet) == false)
            {
                TimeSeries[key] = hashSet = new HashSet<TimeSeriesRange>(TimeSeriesRangeComparer.Instance);
            }

            var (from, to) = TimeSeriesHandler.ParseDates(fromStr, toStr, timeseries);

            hashSet.Add(new TimeSeriesRange
            {
                Name = timeseries,
                From = from,
                To = to
            });
        }
    }
}
