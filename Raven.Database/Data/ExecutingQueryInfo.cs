using System;
using System.Diagnostics;
using System.Threading;

using Raven.Abstractions.Data;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Database.Data
{
    public class ExecutingQueryInfo
    {
        public DateTime StartTime { get; private set; }

        public IndexQuery QueryInfo { get; private set; }

        public long QueryId { get; private set; }

		[JsonIgnore]
        public CancellationTokenSource TokenSource { get; private set; }

        public TimeSpan Duration
        {
            get
            {
                return stopwatch.Elapsed;
            }
        }

        private readonly Stopwatch stopwatch;

        public ExecutingQueryInfo(DateTime startTime, IndexQuery queryInfo, long queryId, CancellationTokenSource tokenSource)
        {
            StartTime = startTime;
            QueryInfo = queryInfo;
            QueryId = queryId;
            stopwatch = Stopwatch.StartNew();
            TokenSource = tokenSource;
        }
    }
}
