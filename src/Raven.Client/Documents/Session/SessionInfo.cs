namespace Raven.Client.Documents.Session
{
    public class SessionInfo
    {
        public int? SessionId { get;}

        public bool AsyncCommandRunning { get; set; }

        public SessionInfo(int sessionId, bool asyncCommandRunning)
        {
            SessionId = sessionId;
            AsyncCommandRunning = asyncCommandRunning;
        }
    }
}
