using System;
using System.Collections.Generic;

namespace Raven.NewClient.Abstractions.TimeSeries
{
    public class TimeSeriesDocument
    {
        /// <summary>
        /// The ID can be either the time series name ("TimeSeriesName") or the full document name ("Raven/TimeSereis/TimeSereisName").
        /// </summary>
        public string Id { get; set; }

        public Dictionary<string, string> Settings { get; set; }

        public Dictionary<string, string> SecuredSettings { get; set; } //preparation for air conditioner

        public bool Disabled { get; set; }

        public TimeSeriesDocument()
        {
            Settings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            SecuredSettings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        }
    }
}
