namespace Raven.Server.Rachis
{
    public class LogLengthNegotiation
    {
        public long PrevLogIndex { get; set; }
        public long PrevLogTerm { get; set; }
        public long Term { get; set; }
        public bool Truncated { get; set; }
        public int SendingThread { get; set; }
    }

    public class LogLengthNegotiationResponse
    {
        public enum ResponseStatus
        {
            Acceptable,
            Negotiation,
            Rejected
        }

        public ResponseStatus Status { get; set; }

        public string Message { get; set; }
        public long CurrentTerm { get; set; }
        public long LastLogIndex { get; set; }

        public long MaxIndex { get; set; }
        public long MinIndex { get; set; }
        public long MidpointIndex { get; set; }
        public long MidpointTerm { get; set; }

        public int? CommandsVersion { get; set; }
    }
}
