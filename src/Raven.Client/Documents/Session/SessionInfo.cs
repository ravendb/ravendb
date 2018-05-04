namespace Raven.Client.Documents.Session
{
    public class SessionInfo
    {
        public int? SessionId { get;}

        public long? LastClusterTransaction { get;}

        public bool AsyncCommandRunning { get; set; }

        public SessionInfo(int sessionId, bool asyncCommandRunning, long? lastClusterTransaction = null)
        {
            LastClusterTransaction = lastClusterTransaction;
            SessionId = sessionId;
            AsyncCommandRunning = asyncCommandRunning;
        }
    }
}
