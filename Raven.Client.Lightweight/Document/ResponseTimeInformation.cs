// -----------------------------------------------------------------------
//  <copyright file="ResponseTimeInformation.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Client.Document
{
    public class ResponseTimeInformation
    {

        public ResponseTimeInformation()
        {
            DurationBreakdown = new List<ResponseTimeItem>();
            TotalServerDuration = new TimeSpan();
            TotalClientDuration = new TimeSpan();
        }
        public TimeSpan TotalServerDuration { get; set; }
        public TimeSpan TotalClientDuration { get; set; }

        public List<ResponseTimeItem> DurationBreakdown { get; set; }

        internal void ComputeServerTotal()
        {
            TotalServerDuration = new TimeSpan(DurationBreakdown.Sum(x => x.Duration.Ticks));
        }
    }

    public class ResponseTimeItem
    {
        public string Url { get; set; }
        public TimeSpan Duration { get; set; }
    }
}