//-----------------------------------------------------------------------
// <copyright file="TimeSeriesValue.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using Sparrow.Json;

namespace Raven.Client.Documents.Session.TimeSeries
{
    public class TimeSeriesEntry : ITimeSeriesValues
    {
        public DateTime Timestamp { get; set; }

        public string Tag { get; set; }

        public double[] Values { get; set; }

        [JsonDeserializationIgnore]
        public double Value
        {
            get => Values[0]; 
            set => Values[0] = value;
        }
    }
    
    public interface ITimeSeriesValues
    {
        public double[] Values { get; set; }
    }
}
