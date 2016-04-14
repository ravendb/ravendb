using System;
using System.Diagnostics;

using Raven.Client.Data;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.Queries
{
    public class ExecutingQueryInfo
    {
        public DateTime StartTime { get; private set; }

        public IndexQuery QueryInfo { get; private set; }

        public long QueryId { get; private set; }

        public OperationCancelToken Token { get; private set; }

        public TimeSpan Duration => stopwatch.Elapsed;

        private readonly Stopwatch stopwatch;

        public ExecutingQueryInfo(DateTime startTime, IndexQuery queryInfo, long queryId, OperationCancelToken token)
        {
            StartTime = startTime;
            QueryInfo = queryInfo;
            QueryId = queryId;
            stopwatch = Stopwatch.StartNew();
            Token = token;
        }
    }
}
