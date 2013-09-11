using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Raven.Abstractions.Data;

namespace Raven.Database.Server.Responders.Debugging
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
