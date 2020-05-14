//-----------------------------------------------------------------------
// <copyright file="TimeSeriesValue.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using Sparrow.Json;

namespace Raven.Client.Documents.Session.TimeSeries
{
    public class TimeSeriesEntry : TimeSeriesEntryValues
    {
        public DateTime Timestamp { get; set; }

        public string Tag { get; set; }

        [JsonDeserializationIgnore]
        public double Value
        {
            get => Values[0]; 
            set => Values[0] = value;
        }
    }

    public abstract class TimeSeriesAggregatedEntry : TimeSeriesEntryValues
    {

    }

    public abstract class TimeSeriesEntryValues
    {
        public double[] Values { get; set; }
    }
}
