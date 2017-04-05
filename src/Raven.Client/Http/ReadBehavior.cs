namespace Raven.Client.Http
{
    public enum ReadBehavior
    {
        CurrentNodeOnly,
        CurrentNodeWithFailover,
        CurrentNodeWithFailoverWhenRequestTimeSlaThresholdIsReached,
        RoundRobin,
        RoundRobinWithFailoverWhenRequestTimeSlaThresholdIsReached,
        FastestNode,
    }
}