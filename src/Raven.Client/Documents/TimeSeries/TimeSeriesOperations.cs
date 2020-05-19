using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session.TimeSeries;
using Sparrow;

namespace Raven.Client.Documents.TimeSeries
{
    public class TimeSeriesOperations
    {
        private readonly IDocumentStore _store;
        private readonly string _database;
        private readonly MaintenanceOperationExecutor _executor;

        public TimeSeriesOperations(IDocumentStore store)
        {
            _store = store;
            _database = store.Database;
            _executor = _store.Maintenance.ForDatabase(_database);
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
            var name = GetTimeSeriesName<TTimeSeriesEntry>();

            var mapping = TimeSeriesEntryValues.GetMembersMapping(typeof(TTimeSeriesEntry));
            if (mapping == null)
                throw new InvalidOperationException($"{typeof(TTimeSeriesEntry).Name} must contain {nameof(TimeSeriesValueAttribute)}.");

            var collection = _store.Conventions.FindCollectionName(typeof(TCollection));

            return Register(collection, name, mapping.Values.Select(f => f.Name).ToArray());
        }
        
        /// <summary>
        /// Register value names of a time-series
        /// </summary>
        public Task Register(string collection, string name, string[] valueNames)
        {
            var command = new ConfigureTimeSeriesValueNamesOperation(collection, name, valueNames);
            return _executor.SendAsync(command);
        }

        /// <summary>
        /// Set rollup and retention policy
        /// </summary>
        /// <typeparam name="TCollection">Collection type</typeparam>
        /// <param name="name">Policy name</param>
        /// <param name="aggregation">Aggregation time</param>
        /// <param name="retention">Retention time</param>
        public Task SetPolicy<TCollection>(string name, TimeValue aggregation, TimeValue retention)
        {
            var collection = _store.Conventions.FindCollectionName(typeof(TCollection));
            return SetPolicy(collection, name, aggregation, retention);
        }

        /// <summary>
        /// Set rollup and retention policy
        /// </summary>
        /// <param name="name">Policy name</param>
        /// <param name="aggregation">Aggregation time</param>
        /// <param name="retention">Retention time</param>
        public Task SetPolicy(string collection, string name, TimeValue aggregation, TimeValue retention)        
        {
            var p = new TimeSeriesPolicy(name, aggregation, retention);
            return _executor.SendAsync(new ConfigureTimeSeriesPolicyOperation(collection, p));
        }

        /// <summary>
        /// Set raw retention policy 
        /// </summary>
        /// <typeparam name="TCollection">Collection type</typeparam>
        /// <param name="retention">Retention time</param>
        public Task SetRawPolicy<TCollection>(TimeValue retention)
        {
            var collection = _store.Conventions.FindCollectionName(typeof(TCollection));
            return SetRawPolicy(collection, retention);
        }

        /// <summary>
        /// Set raw retention policy
        /// </summary>
        /// <param name="retention">Retention time</param>
        /// <returns></returns>
        public Task SetRawPolicy(string collection, TimeValue retention)
        {
            var p = new RawTimeSeriesPolicy(retention);
            return _executor.SendAsync(new ConfigureRawTimeSeriesPolicyOperation(collection, p));
        }

        /// <summary>
        /// Remove policy
        /// </summary>
        /// <param name="name">Policy name</param>
        public Task RemovePolicy(string collection, string name)
        {
            return _executor.SendAsync(new RemoveTimeSeriesPolicyOperation(collection, name));
        }

        /// <summary>
        /// Remove policy
        /// </summary>
        /// <typeparam name="TCollection">Collection type</typeparam>
        /// <param name="name">Policy name</param>
        /// <returns></returns>
        public Task RemovePolicy<TCollection>(string name)
        {
            var collection = _store.Conventions.FindCollectionName(typeof(TCollection));
            return RemovePolicy(collection, name);
        }

        internal static string GetTimeSeriesName<TTimeSeriesEntry>() where TTimeSeriesEntry : TimeSeriesEntry
        {
            if (typeof(TTimeSeriesEntry) == typeof(TimeSeriesEntry))
                throw new InvalidOperationException($"Only derived class from '{nameof(TimeSeriesEntry)}' can be registered.");

            return typeof(TTimeSeriesEntry).Name;
        }

        public TimeSeriesOperations ForDatabase(string database)
        {
            if (string.Equals(_database, database, StringComparison.OrdinalIgnoreCase))
                return this;

            return new TimeSeriesOperations(_store, database);
        }
    }
}
