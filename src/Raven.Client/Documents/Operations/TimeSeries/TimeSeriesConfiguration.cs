using System;
using System.Collections.Generic;
using System.Text;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public class TimeSeriesConfiguration
    {
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
