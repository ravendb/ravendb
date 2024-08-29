namespace Raven.Client.Http
{
    /// <summary>
    /// Defines the behavior for load balancing client requests across multiple nodes.
    /// </summary>
    /// <value>
    /// <list type="bullet">
    /// <item>
    /// <term>None</term>
    /// <description>No load balancing is applied; all requests are sent to the preferred node.</description>
    /// </item>
    /// <item>
    /// <term>UseSessionContext</term>
    /// <description>Requests are distributed based on the session context, balancing the load across available nodes.</description>
    /// </item>
    /// </list>
    /// </value>
    public enum LoadBalanceBehavior
    {
        None,
        UseSessionContext
    }
}
