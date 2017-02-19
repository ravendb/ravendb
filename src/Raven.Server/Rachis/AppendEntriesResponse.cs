namespace Raven.Server.Rachis
{
    public class AppendEntriesResponse
    {
        public long LastLogIndex { get; set; }

        public long CurrentTerm { get; set; }

        public bool Success { get; set; } 

        public string Message { get; set; }

        //This is used when a follower and a leader need to agree on a matched index
        public Negotiation Negotiation { get; set; }
    }
}
