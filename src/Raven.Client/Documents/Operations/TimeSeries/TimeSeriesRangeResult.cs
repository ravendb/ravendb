﻿using System;
using Raven.Client.Documents.Session;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public class TimeSeriesRangeResult
    {
        public string Name;
        public DateTime From, To;
        public TimeSeriesValue[] Values;
        public bool FullRange;
    }
}
