using System;
using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public class TimeSeriesConfiguration : IDynamicJson
    {
        internal const char TimeSeriesRollupSeparator = '@';
        public Dictionary<string, TimeSeriesCollectionConfiguration> Collections { get; set; }

        public TimeSpan PolicyCheckFrequency { get; set; } = TimeSpan.FromMinutes(10);

        public ValueNameMapper ValueNameMapper { get; set; }

        internal void Initialize()
        {
            if (Collections == null) 
                return;

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
                [nameof(ValueNameMapper)] = ValueNameMapper.ToJson()
            };
        }
    }
    
    public class ValueNameMapper : IDynamicJson
    {
        // collection -> timeseries -> names
        internal Dictionary<string, Dictionary<string, string[]>> CollectionHolder = new Dictionary<string, Dictionary<string, string[]>>();

        public ValueNameMapper()
        {
            // for de-serializer
        }

        public ValueNameMapper(string collection, string timeSeries, string[] names)
        {
            AddValueName(collection, timeSeries, names);
        }

        public void AddValueName(string collection, string timeSeries, string[] names)
        {
            if (CollectionHolder.TryGetValue(collection, out var timeSeriesHolder) == false)
                timeSeriesHolder = CollectionHolder[collection] = new Dictionary<string, string[]>();

            timeSeriesHolder[timeSeries] = names;
        }

        public bool TryAddValueName(string collection, string timeSeries, string[] names)
        {
            if (CollectionHolder.TryGetValue(collection, out var timeSeriesHolder) == false)
                timeSeriesHolder = CollectionHolder[collection] = new Dictionary<string, string[]>();

            if (timeSeriesHolder.ContainsKey(timeSeries))
                return false;

            timeSeriesHolder[timeSeries] = names;
            return true;
        }

        public string[] GetNames(string collection, string timeSeries)
        {
            if (CollectionHolder.TryGetValue(collection, out var timeSeriesHolder) == false)
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
            foreach (var collection in CollectionHolder)
            {
                djv[collection.Key] = DynamicJsonValue.Convert(collection.Value);
            }

            return new DynamicJsonValue
            {
                [nameof(CollectionHolder)] = djv
            };
        }
    }
}
