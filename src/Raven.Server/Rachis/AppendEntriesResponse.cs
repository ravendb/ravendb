namespace Raven.Server.Rachis
{
    public sealed class AppendEntriesResponse
    {
        public long LastLogIndex { get; set; }

        public long LastCommitIndex { get; set; }

        public long CurrentTerm { get; set; }

        public bool Success { get; set; } 
        
        public bool Pending { get; set; }

        public string Message { get; set; }

        public override string ToString()
        {
            return $"Replying with {nameof(Success)}: {Success}, {nameof(Pending)}: {Pending}, {nameof(Message)}: {Message} ({CurrentTerm} / {LastLogIndex}, commit:{LastCommitIndex})";
        }

        public bool Equals(AppendEntriesResponse aer)
        {
            return aer != null &&
                   aer.LastLogIndex == this.LastLogIndex &&
                   aer.LastCommitIndex == this.LastCommitIndex &&
                   aer.CurrentTerm == this.CurrentTerm &&
                   aer.Success == this.Success &&
                   aer.Pending == this.Pending &&
                   aer.Message == this.Message;
        }
    }
}
