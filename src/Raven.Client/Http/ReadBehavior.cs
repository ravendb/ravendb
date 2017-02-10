namespace Raven.Client.Http
{
    public enum ReadBehavior
    {
        LeaderOnly,
        LeaderWithFailover,
        LeaderWithFailoverWhenRequestTimeSlaThresholdIsReached,
        RoundRobin,
        RoundRobinWithFailoverWhenRequestTimeSlaThresholdIsReached,
        FastestNode,
    }
}