using System;

namespace Raven.Client.Documents.Session
{
    public class SessionInfo
    {
        public int? SessionId { get;}

        public Func<long?> GetLastClusterTransactionFunc;

        public bool AsyncCommandRunning { get; set; }

        public SessionInfo(int sessionId, bool asyncCommandRunning, Func<long?> getLastClusterTransactionFunc = null)
        {
            GetLastClusterTransactionFunc = getLastClusterTransactionFunc;
            SessionId = sessionId;
            AsyncCommandRunning = asyncCommandRunning;
        }
    }
}
