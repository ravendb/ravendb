namespace Raven.Client.Documents.Session
{
    public class SessionInfo
    {
        public int? SessionId { get;}

        public long? LastClusterTransactionIndex { get; set; }

        public bool AsyncCommandRunning { get; set; }

        public bool NoCaching { get; set; }

        public SessionInfo(int sessionId, bool asyncCommandRunning, long? lastClusterTransactionIndex = null, bool noCaching = false)
        {
            LastClusterTransactionIndex = lastClusterTransactionIndex;
            SessionId = sessionId;
            AsyncCommandRunning = asyncCommandRunning;
            NoCaching = noCaching;
        }
    }
}
