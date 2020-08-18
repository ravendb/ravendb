using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client.Util;
using Sparrow;

namespace Raven.Client.Documents.TimeSeries
{
    public class TimeSeriesOperations
    {
        private readonly IDocumentStore _store;
        private readonly string _database;
        private readonly MaintenanceOperationExecutor _executor;

        public TimeSeriesOperations(IDocumentStore store) : this(store, store.Database)
        {
        }

        private TimeSeriesOperations(IDocumentStore store, string database)
        {
            _store = store;
            _database = database;
            _executor = _store.Maintenance.ForDatabase(_database);
        }

        /// <summary>
        /// Register value names of a time-series
        /// </summary>
        /// <typeparam name="TCollection">Collection type</typeparam>
        /// <typeparam name="TTimeSeriesEntry">Time-series type</typeparam>
        public Task RegisterAsync<TCollection, TTimeSeriesEntry>(string name = null)
        {
            name ??= GetTimeSeriesName<TTimeSeriesEntry>(_store.Conventions);

            var mapping = TimeSeriesValuesHelper.GetMembersMapping(typeof(TTimeSeriesEntry));
            if (mapping == null)
                throw new InvalidOperationException($"{typeof(TTimeSeriesEntry).Name} must contain {nameof(TimeSeriesValueAttribute)}.");

            var collection = _store.Conventions.FindCollectionName(typeof(TCollection));

            return RegisterAsync(collection, name, mapping.Values.Select(f => f.Name).ToArray());
        }

        /// <summary>
        /// Register value names of a time-series
        /// </summary>
        /// <typeparam name="TCollection">Collection type</typeparam>
        public Task RegisterAsync<TCollection>(string name, string[] valueNames)
        {
            var collection = _store.Conventions.FindCollectionName(typeof(TCollection));
            return RegisterAsync(collection, name, valueNames);
        }

        /// <summary>
        /// Register value names of a time-series
        /// </summary>
        public Task RegisterAsync(string collection, string name, string[] valueNames)
        {
            var parameters = new ConfigureTimeSeriesValueNamesOperation.Parameters
            {
                Collection = collection,
                TimeSeries = name,
                ValueNames = valueNames,
                Update = true
            };
            var command = new ConfigureTimeSeriesValueNamesOperation(parameters);
            return _executor.SendAsync(command);
        }

        /// <summary>
        /// Set rollup and retention policy
        /// </summary>
        /// <typeparam name="TCollection">Collection type</typeparam>
        /// <param name="name">Policy name</param>
        /// <param name="aggregation">Aggregation time</param>
        /// <param name="retention">Retention time</param>
        public Task SetPolicyAsync<TCollection>(string name, TimeValue aggregation, TimeValue retention)
        {
            var collection = _store.Conventions.FindCollectionName(typeof(TCollection));
            return SetPolicyAsync(collection, name, aggregation, retention);
        }

        /// <summary>
        /// Set rollup and retention policy
        /// </summary>
        /// <param name="name">Policy name</param>
        /// <param name="aggregation">Aggregation time</param>
        /// <param name="retention">Retention time</param>
        public Task SetPolicyAsync(string collection, string name, TimeValue aggregation, TimeValue retention)
        {
            var p = new TimeSeriesPolicy(name, aggregation, retention);
            return _executor.SendAsync(new ConfigureTimeSeriesPolicyOperation(collection, p));
        }

        /// <summary>
        /// Set raw retention policy
        /// </summary>
        /// <typeparam name="TCollection">Collection type</typeparam>
        /// <param name="retention">Retention time</param>
        public Task SetRawPolicyAsync<TCollection>(TimeValue retention)
        {
            var collection = _store.Conventions.FindCollectionName(typeof(TCollection));
            return SetRawPolicyAsync(collection, retention);
        }

        /// <summary>
        /// Set raw retention policy
        /// </summary>
        /// <param name="retention">Retention time</param>
        /// <returns></returns>
        public Task SetRawPolicyAsync(string collection, TimeValue retention)
        {
            var p = new RawTimeSeriesPolicy(retention);
            return _executor.SendAsync(new ConfigureRawTimeSeriesPolicyOperation(collection, p));
        }

        /// <summary>
        /// Remove policy
        /// </summary>
        /// <param name="name">Policy name</param>
        public Task RemovePolicyAsync(string collection, string name)
        {
            return _executor.SendAsync(new RemoveTimeSeriesPolicyOperation(collection, name));
        }

        /// <summary>
        /// Remove policy
        /// </summary>
        /// <typeparam name="TCollection">Collection type</typeparam>
        /// <param name="name">Policy name</param>
        /// <returns></returns>
        public Task RemovePolicyAsync<TCollection>(string name)
        {
            var collection = _store.Conventions.FindCollectionName(typeof(TCollection));
            return RemovePolicyAsync(collection, name);
        }

        /// <summary>
        /// Register value names of a time-series
        /// </summary>
        /// <typeparam name="TCollection">Collection type</typeparam>
        /// <typeparam name="TTimeSeriesEntry">Time-series type</typeparam>
        public void Register<TCollection, TTimeSeriesEntry>(string name = null)
        {
            AsyncHelpers.RunSync(() => RegisterAsync<TCollection, TTimeSeriesEntry>(name));
        }

        /// <summary>
        /// Register value names of a time-series
        /// </summary>
        /// <typeparam name="TCollection">Collection type</typeparam>
        public void Register<TCollection>(string name, string[] valueNames)
        {
            AsyncHelpers.RunSync(() => RegisterAsync<TCollection>(name, valueNames));
        }

        /// <summary>
        /// Register value names of a time-series
        /// </summary>
        public void Register(string collection, string name, string[] valueNames)
        {
            AsyncHelpers.RunSync(() => RegisterAsync(collection, name, valueNames));
        }

        /// <summary>
        /// Set rollup and retention policy
        /// </summary>
        /// <typeparam name="TCollection">Collection type</typeparam>
        /// <param name="name">Policy name</param>
        /// <param name="aggregation">Aggregation time</param>
        /// <param name="retention">Retention time</param>
        public void SetPolicy<TCollection>(string name, TimeValue aggregation, TimeValue retention)
        {
            AsyncHelpers.RunSync(() => SetPolicyAsync<TCollection>(name, aggregation, retention));
        }

        /// <summary>
        /// Set rollup and retention policy
        /// </summary>
        /// <param name="name">Policy name</param>
        /// <param name="aggregation">Aggregation time</param>
        /// <param name="retention">Retention time</param>
        public void SetPolicy(string collection, string name, TimeValue aggregation, TimeValue retention)
        {
            AsyncHelpers.RunSync(() => SetPolicyAsync(collection, name, aggregation, retention));
        }

        /// <summary>
        /// Set raw retention policy
        /// </summary>
        /// <typeparam name="TCollection">Collection type</typeparam>
        /// <param name="retention">Retention time</param>
        public void SetRawPolicy<TCollection>(TimeValue retention)
        {
            AsyncHelpers.RunSync(() => SetRawPolicyAsync<TCollection>(retention));
        }

        /// <summary>
        /// Set raw retention policy
        /// </summary>
        /// <param name="retention">Retention time</param>
        /// <returns></returns>
        public void SetRawPolicy(string collection, TimeValue retention)
        {
            AsyncHelpers.RunSync(() => SetRawPolicyAsync(collection, retention));
        }

        /// <summary>
        /// Remove policy
        /// </summary>
        /// <param name="name">Policy name</param>
        public void RemovePolicy(string collection, string name)
        {
            AsyncHelpers.RunSync(() => RemovePolicyAsync(collection, name));
        }

        /// <summary>
        /// Remove policy
        /// </summary>
        /// <typeparam name="TCollection">Collection type</typeparam>
        /// <param name="name">Policy name</param>
        /// <returns></returns>
        public void RemovePolicy<TCollection>(string name)
        {
            AsyncHelpers.RunSync(() => RemovePolicyAsync<TCollection>(name));
        }

        internal static string GetTimeSeriesName<TTimeSeriesEntry>(DocumentConventions conventions)
        {
            return conventions.GetCollectionName(typeof(TTimeSeriesEntry));
        }

        public TimeSeriesOperations ForDatabase(string database)
        {
            if (string.Equals(_database, database, StringComparison.OrdinalIgnoreCase))
                return this;

            return new TimeSeriesOperations(_store, database);
        }
    }
}
