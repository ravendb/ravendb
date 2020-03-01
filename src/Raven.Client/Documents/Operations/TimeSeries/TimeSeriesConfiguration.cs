using System;
using System.Collections.Generic;
using System.Text;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public class TimeSeriesConfiguration
    {
        public Dictionary<string, TimeSeriesCollectionConfiguration> Collections { get; set; }
    }
}
