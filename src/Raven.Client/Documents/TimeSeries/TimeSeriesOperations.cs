using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session.TimeSeries;

namespace Raven.Client.Documents.TimeSeries
{
    public class TimeSeriesOperations
    {
        private readonly IDocumentStore _store;
        private readonly string _database;

        public TimeSeriesOperations(IDocumentStore store)
        {
            _store = store;
            _database = store.Database;
        }

        private TimeSeriesOperations(IDocumentStore store, string database)
        {
            _store = store;
            _database = database;
        }

        /// <summary>
        /// Register value names of a time-series
        /// </summary>
        /// <typeparam name="TCollection">Collection type</typeparam>
        /// <typeparam name="TTimeSeriesEntry">Time-series type, must be derived from TimeSeriesEntry class</typeparam>
        public Task Register<TCollection, TTimeSeriesEntry>() where TTimeSeriesEntry : TimeSeriesEntry
        {
            if (typeof(TTimeSeriesEntry) == typeof(TimeSeriesEntry))
                throw new InvalidOperationException($"Only derived class from '{nameof(TimeSeriesEntry)}' can be registered.");

            var mapping = TimeSeriesEntryValues.GetFieldsMapping(typeof(TTimeSeriesEntry));
            if (mapping == null)
                throw new InvalidOperationException($"{typeof(TTimeSeriesEntry).Name} must contain {nameof(TimeSeriesValueAttribute)}.");

            var collection = _store.Conventions.FindCollectionName(typeof(TCollection));

            return Register(collection, typeof(TTimeSeriesEntry).Name, mapping.Values.Select(f => f.Name).ToArray());
        }

        /// <summary>
        /// Register value names of a time-series
        /// </summary>
        public Task Register(string collection, string name, string[] valueNames)
        {
            var command = new ConfigureTimeSeriesValueNamesOperation(collection, name, valueNames);
            return _store.Maintenance.ForDatabase(_database).SendAsync(command);
        }

        public TimeSeriesOperations ForDatabase(string database)
        {
            if (string.Equals(_database, database, StringComparison.OrdinalIgnoreCase))
                return this;

            return new TimeSeriesOperations(_store, database);
        }
    }
}
