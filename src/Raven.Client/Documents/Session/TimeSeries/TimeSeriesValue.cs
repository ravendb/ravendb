//-----------------------------------------------------------------------
// <copyright file="TimeSeriesValue.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;

namespace Raven.Client.Documents.Session.TimeSeries
{
    public class TimeSeriesValue
    {
        public DateTime Timestamp { get; set; }

        public string Tag { get; set; }

        public double[] Values { get; set; }

        public double Value => Values[0];
    }
}
