using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public class TimeSeriesConfiguration : IDynamicJson
    {
        internal const char TimeSeriesRollupSeparator = '@';
        public Dictionary<string, TimeSeriesCollectionConfiguration> Collections { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Collections)] = DynamicJsonValue.Convert(Collections)
            };
        }
    }
}
