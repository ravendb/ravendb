namespace Raven.Server.Rachis
{
    public class AppendEntriesResponse
    {
        public long LastLogIndex { get; set; }

        public long CurrentTerm { get; set; }

        public bool Success { get; set; } 

        public string Message { get; set; }
    }
}
