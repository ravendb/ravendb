using System;
using System.Diagnostics;
using Raven.Client.Documents.Queries;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.Queries
{
    public class ExecutingQueryInfo
    {
        public DateTime StartTime { get; }

        public IIndexQuery QueryInfo { get; }

        public long QueryId { get; }

        public OperationCancelToken Token { get; }

        public TimeSpan Duration => _stopwatch.Elapsed;

        private readonly Stopwatch _stopwatch;

        public ExecutingQueryInfo(DateTime startTime, IIndexQuery queryInfo, long queryId, OperationCancelToken token)
        {
            StartTime = startTime;
            QueryInfo = queryInfo;
            QueryId = queryId;
            _stopwatch = Stopwatch.StartNew();
            Token = token;
        }
    }
}
