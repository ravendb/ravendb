using System;
using System.Diagnostics;

using Raven.Abstractions.Data;

namespace Raven.Database.Data
{
    public class ExecutingQueryInfo
    {
        public DateTime StartTime { get; private set; }

        public IndexQuery QueryInfo { get; private set; }

        public TimeSpan Duration
        {
            get
            {
                return stopwatch.Elapsed;
            }
        }

        private readonly Stopwatch stopwatch;

        public ExecutingQueryInfo(DateTime startTime, IndexQuery queryInfo)
        {
            StartTime = startTime;
            QueryInfo = queryInfo;
            stopwatch = Stopwatch.StartNew();
        }
    }
}
