namespace Raven.Client.Http
{
    public enum ReadBehavior
    {
        ReadFromLeaderOnly,
        ReadFromLeaderWithFailover,
        ReadFromLeaderWithFailoverWhenRequestTimeSlaThresholdIsReached,
        ReadFromRandomNode,
        ReadFromRandomNodeWithFailoverWhenRequestTimeSlaThresholdIsReached,
        ReadFromFastestNode,
    }
}