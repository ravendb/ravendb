namespace Raven.Client.Http
{
    public enum ReadBehavior
    {
        ConversationNodeOnly,
        ConversationNodeWithFailover,
        ConversationNodeWithFailoverWhenRequestTimeSlaThresholdIsReached,
        RoundRobin,
        RoundRobinWithFailoverWhenRequestTimeSlaThresholdIsReached,
        FastestNode,
    }
}