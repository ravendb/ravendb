using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using Newtonsoft.Json;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public class TimeSeriesConfiguration : IDynamicJson
    {
        internal const char TimeSeriesRollupSeparator = '@';
        public Dictionary<string, TimeSeriesCollectionConfiguration> Collections { get; set; }

        public TimeSpan? PolicyCheckFrequency { get; set; }

        public TimeSeriesValueNameMapper ValueNameMapper { get; set; }

        internal void InitializeRollupAndRetention()
        {
            if (Collections == null) 
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
            return new DynamicJsonValue
            {
                [nameof(Collections)] = DynamicJsonValue.Convert(Collections),
                [nameof(PolicyCheckFrequency)] = PolicyCheckFrequency,
                [nameof(ValueNameMapper)] = ValueNameMapper?.ToJson()
            };
        }
    }
    
    public class TimeSeriesValueNameMapper : IDynamicJson, IPostJsonDeserialization
    {
        // collection -> timeseries -> names
        public Dictionary<string, Dictionary<string, string[]>> Mapping { get; set; } =
            new Dictionary<string, Dictionary<string, string[]>>(StringComparer.OrdinalIgnoreCase);

        public TimeSeriesValueNameMapper()
        {
            // for de-serializer
        }

        public TimeSeriesValueNameMapper(string collection, string timeSeries, string[] names)
        {
            AddValueName(collection, timeSeries, names);
        }

        public void AddValueName(string collection, string timeSeries, string[] names)
        {
            if (Mapping.TryGetValue(collection, out var timeSeriesHolder) == false)
                timeSeriesHolder = Mapping[collection] = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);

            timeSeriesHolder[timeSeries] = names;
        }

        public bool TryAddValueName(string collection, string timeSeries, string[] names)
        {
            if (Mapping.TryGetValue(collection, out var timeSeriesHolder) == false)
                timeSeriesHolder = Mapping[collection] = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);

            if (timeSeriesHolder.ContainsKey(timeSeries))
                return false;

            timeSeriesHolder[timeSeries] = names;
            return true;
        }

        public string[] GetNames(string collection, string timeSeries)
        {
            if (Mapping.TryGetValue(collection, out var timeSeriesHolder) == false)
                return null;

            if (timeSeriesHolder.ContainsKey(timeSeries) == false)
                return null;

            if (timeSeriesHolder.TryGetValue(timeSeries, out var names) == false)
                return null;

            return names;
        }

        public DynamicJsonValue ToJson()
        {
            var djv = new DynamicJsonValue();
            foreach (var collection in Mapping)
            {
                djv[collection.Key] = DynamicJsonValue.Convert(collection.Value);
            }

            return new DynamicJsonValue
            {
                [nameof(Mapping)] = djv
            };
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
            // ensure StringComparer.InvariantCultureIgnoreCase
            var dic = new Dictionary<string, Dictionary<string, string[]>>(StringComparer.OrdinalIgnoreCase);
            foreach (var kvp in Mapping)
            {
                dic[kvp.Key] = new Dictionary<string, string[]>(kvp.Value, StringComparer.OrdinalIgnoreCase);
            }

            Mapping = dic;
        }
    }
}
