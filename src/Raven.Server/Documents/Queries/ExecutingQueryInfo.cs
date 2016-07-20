using System;
using System.Diagnostics;

using Raven.Server.ServerWide;

namespace Raven.Server.Documents.Queries
{
    public class ExecutingQueryInfo
    {
        public DateTime StartTime { get; private set; }

        public IndexQueryServerSide QueryInfo { get; private set; }

        public long QueryId { get; private set; }

        public OperationCancelToken Token { get; private set; }

        public TimeSpan Duration => _stopwatch.Elapsed;

        private readonly Stopwatch _stopwatch;

        public ExecutingQueryInfo(DateTime startTime, IndexQueryServerSide queryInfo, long queryId, OperationCancelToken token)
        {
            StartTime = startTime;
            QueryInfo = queryInfo;
            QueryId = queryId;
            _stopwatch = Stopwatch.StartNew();
            Token = token;
        }
    }
}
