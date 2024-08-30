namespace Raven.Client.Http
{
    /// <summary>
    /// Defines the behavior for balancing read operations across multiple nodes.
    /// </summary>
    /// <value>
    /// <list type="bullet">
    /// <item>
    /// <term>None</term>
    /// <description>No read balancing is applied; all read operations are sent to the preferred node.</description>
    /// </item>
    /// <item>
    /// <term>RoundRobin</term>
    /// <description>Read operations are distributed across nodes in a round-robin fashion, balancing the load.</description>
    /// </item>
    /// <item>
    /// <term>FastestNode</term>
    /// <description>Read operations are directed to the fastest node available at the time of the request.</description>
    /// </item>
    /// </list>
    /// </value>
    public enum ReadBalanceBehavior
    {
        None,
        RoundRobin,
        FastestNode
    }
}
