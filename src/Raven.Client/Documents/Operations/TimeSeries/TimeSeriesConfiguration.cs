using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public class TimeSeriesConfiguration : IDynamicJson, IPostJsonDeserialization
    {
        internal const char TimeSeriesRollupSeparator = '@';
        public Dictionary<string, TimeSeriesCollectionConfiguration> Collections { get; set; }

        public TimeSpan? PolicyCheckFrequency { get; set; }

        // collection -> timeseries -> names
        public Dictionary<string, Dictionary<string, string[]>> NamedValues { get; set; }
        
        internal void InitializeRollupAndRetention()
        {
            if (Collections == null || Collections.Count == 0) 
                return;

            PolicyCheckFrequency ??= TimeSpan.FromMinutes(10);

            if (PolicyCheckFrequency <= TimeSpan.Zero)
                throw new ArgumentException($"{nameof(PolicyCheckFrequency)} must be positive.");

            foreach (var config in Collections.Values)
            {
                config?.ValidateAndInitialize();
            }
        }

        public DynamicJsonValue ToJson()
        {
            var json = new DynamicJsonValue
            {
                [nameof(Collections)] = DynamicJsonValue.Convert(Collections),
                [nameof(PolicyCheckFrequency)] = PolicyCheckFrequency,
            };

            if (NamedValues != null)
            {
                var djv = new DynamicJsonValue();
                foreach (var collection in NamedValues)
                {
                    djv[collection.Key] = DynamicJsonValue.Convert(collection.Value);
                }

                json[nameof(NamedValues)] = djv;
            }

            return json;
        }

        public void AddValueName(string collection, string timeSeries, string[] names)
        {
            NamedValues ??= new Dictionary<string, Dictionary<string, string[]>>(StringComparer.OrdinalIgnoreCase);
            if (NamedValues.TryGetValue(collection, out var timeSeriesHolder) == false)
                timeSeriesHolder = NamedValues[collection] = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);

            timeSeriesHolder[timeSeries] = names;
        }

        public bool TryAddValueName(string collection, string timeSeries, string[] names)
        {
            NamedValues ??= new Dictionary<string, Dictionary<string, string[]>>(StringComparer.OrdinalIgnoreCase);
            if (NamedValues.TryGetValue(collection, out var timeSeriesHolder) == false)
                timeSeriesHolder = NamedValues[collection] = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);

            if (timeSeriesHolder.ContainsKey(timeSeries))
                return false;

            timeSeriesHolder[timeSeries] = names;
            return true;
        }

        public string[] GetNames(string collection, string timeSeries)
        {
            if (NamedValues == null)
                return null;

            if (NamedValues.TryGetValue(collection, out var timeSeriesHolder) == false)
                return null;

            if (timeSeriesHolder.ContainsKey(timeSeries) == false)
                return null;

            if (timeSeriesHolder.TryGetValue(timeSeries, out var names) == false)
                return null;

            return names;
        }

        [OnDeserialized]
        internal void OnNewtonSoftJsonDeserialized(StreamingContext context)
        {
            InternalPostJsonDeserialization();
        }

        void IPostJsonDeserialization.PostDeserialization()
        {
            InternalPostJsonDeserialization();
        }

        private void InternalPostJsonDeserialization()
        {
            if(NamedValues == null)
                return;
            
            // ensure StringComparer.OrdinalIgnoreCase
            var dic = new Dictionary<string, Dictionary<string, string[]>>(StringComparer.OrdinalIgnoreCase);
            foreach (var kvp in NamedValues)
            {
                dic[kvp.Key] = new Dictionary<string, string[]>(kvp.Value, StringComparer.OrdinalIgnoreCase);
            }

            NamedValues = dic;
        }
    }
}
