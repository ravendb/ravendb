// -----------------------------------------------------------------------
//  <copyright file="TrafficRecorderConfiguration.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Net;
using Raven.Client.Util;

namespace Raven.Traffic
{
    public class TrafficToolConfiguration
    {
        public TrafficToolMode Mode { get; set; }
        public string RecordFilePath { get; set; }
        public TimeSpan Timeout { get; set; }
        public bool IsCompressed { get; set; }
        public bool PrintOutput { get; set; }
        public int? AmountConstraint { get; set; }
        public TimeSpan? DurationConstraint { get; set; }

        public string Database { get; set; }

        public TrafficToolConfiguration()
        {
            IsCompressed = false;
            Timeout = TimeSpan.MinValue;
            PrintOutput = true;
        }

        public enum TrafficToolMode
        {
            Record,
            Replay
        }

        public class RecordConstraint
        {
            public enum ConstraintType
            {
                Time,
                Amount
            }

            public ConstraintType Type { get; set; }
            public int Amount { get; set; }
            public TimeSpan Length { get; set; }

        }
    }
}
