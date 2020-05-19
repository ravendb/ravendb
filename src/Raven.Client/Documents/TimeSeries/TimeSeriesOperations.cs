using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session.TimeSeries;

namespace Raven.Client.Documents.TimeSeries
{
    public class TimeSeriesOperations
    {
        private readonly IDocumentStore _store;

        public TimeSeriesOperations(IDocumentStore store)
        {
            _store = store;
        }

        /// <summary>
        /// Register value names of a time-series
        /// </summary>
        /// <typeparam name="TCollection">Collection type</typeparam>
        /// <typeparam name="TTimeSeriesEntry">Time-series type, must be derived from TimeSeriesEntry class</typeparam>
        public Task Register<TCollection, TTimeSeriesEntry>(string database = null) where TTimeSeriesEntry : TimeSeriesEntry
        {
            if (typeof(TTimeSeriesEntry) == typeof(TimeSeriesEntry))
                throw new InvalidOperationException($"Only derived class from '{nameof(TimeSeriesEntry)}' can be registered.");

            var mapping = TimeSeriesEntryValues.GetFieldsMapping(typeof(TTimeSeriesEntry));
            if (mapping == null)
                throw new InvalidOperationException($"{typeof(TTimeSeriesEntry).Name} must contain {nameof(TimeSeriesValueAttribute)}.");

            var collection = _store.Conventions.FindCollectionName(typeof(TCollection));
            var command = new ConfigureTimeSeriesValueNamesOperation(collection, typeof(TTimeSeriesEntry).Name, mapping.Values.Select(f => f.Name).ToArray());

            if (database != null)
                return _store.Maintenance.ForDatabase(database).SendAsync(command);

            return _store.Maintenance.SendAsync(command);
        }

        /// <summary>
        /// Register value names of a time-series
        /// </summary>
        public Task Register(string collection, string name, string[] valueNames, string database = null)
        {
            var command = new ConfigureTimeSeriesValueNamesOperation(collection, name, valueNames);

            if (database != null)
                return _store.Maintenance.ForDatabase(database).SendAsync(command);

            return _store.Maintenance.SendAsync(command);
        }
    }
}
