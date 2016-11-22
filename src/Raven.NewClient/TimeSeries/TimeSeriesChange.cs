// -----------------------------------------------------------------------
//  <copyright file="TimeSeriesChanges.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace Raven.NewClient.Abstractions.TimeSeries
{
    public class TimeSeriesAppend
    {
        public string Type { get; set; }
        public string Key { get; set; }
        public DateTimeOffset At { get; set; }
        public double[] Values { get; set; }

        [JsonIgnore]
        public TaskCompletionSource<object> Done { get; set; }
    }

    public class TimeSeriesDelete
    {
        public string Type { get; set; }
        public string Key { get; set; }

        [JsonIgnore]
        public TaskCompletionSource<object> Done { get; set; }
    }

    public class TimeSeriesDeleteRange
    {
        public string Type { get; set; }
        public string Key { get; set; }
        public DateTimeOffset Start { get; set; }
        public DateTimeOffset End { get; set; }

        [JsonIgnore]
        public TaskCompletionSource<object> Done { get; set; }
    }
}
