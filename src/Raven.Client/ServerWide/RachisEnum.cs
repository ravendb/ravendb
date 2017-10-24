namespace Raven.Client.ServerWide
{
    public enum RachisState
    {
        Passive,
        Candidate,
        Follower,
        LeaderElect,
        Leader
    }
}
