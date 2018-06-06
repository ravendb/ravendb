namespace Raven.Server.Rachis
{
    public enum ElectionResult
    {
        InProgress,
        Won,
        Lost
    }

    public class RequestVote
    {
        public int SendingThread { get; set; }
        public long Term { get; set; }
        public long LastLogIndex { get; set; }
        public long LastLogTerm { get; set; }
        public bool IsTrialElection { get; set; }
        public bool IsForcedElection { get; set; }
        public string Source { get; set; }
        public ElectionResult ElectionResult { get; set; }
    }

    public class RequestVoteResponse
    {
        public long Term { get; set; }
        public bool VoteGranted { get; set; }
        public bool NotInTopology { get; set; }
        public int? ClusterCommandsVersion { get; set; }
        public string Message { get; set; }
    }
}
