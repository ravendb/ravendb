using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public class TimeSeriesConfiguration : IDynamicJson
    {
        internal const char TimeSeriesRollupSeparator = '@';
        public Dictionary<string, TimeSeriesCollectionConfiguration> Collections { get; set; }

        public TimeSpan PolicyCheckFrequency { get; set; } = TimeSpan.FromMinutes(10);

        internal void Initialize()
        {
            if (Collections == null) 
                return;

            if (PolicyCheckFrequency <= TimeSpan.Zero)
                throw new ArgumentException($"{nameof(PolicyCheckFrequency)} must be positive.");

            foreach (var config in Collections.Values)
            {
                config?.Initialize();
            }
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Collections)] = DynamicJsonValue.Convert(Collections),
                [nameof(PolicyCheckFrequency)] = PolicyCheckFrequency
            };
        }
    }
}
