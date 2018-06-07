using System;

namespace Raven.Client.Documents.Session
{
    public class SessionInfo
    {
        public int? SessionId { get;}

        public long? LastClusterTransactionIndex { get; set; }

        public bool AsyncCommandRunning { get; set; }

        public SessionInfo(int sessionId, bool asyncCommandRunning, long? lastClusterTransactionIndex = null)
        {
            LastClusterTransactionIndex = lastClusterTransactionIndex;
            SessionId = sessionId;
            AsyncCommandRunning = asyncCommandRunning;
        }
    }
}
