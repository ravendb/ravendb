namespace Raven.Server.Routing
{
    public enum CorsMode
    {
        /// <summary>
        /// Never send CORS headers
        /// </summary>
        None,
        
        /// <summary>
        /// Send CORS headers is XHR request comes from cluster
        /// </summary>
        Cluster,
        
        /// <summary>
        /// Send CORS headers regardless user origin
        /// </summary>
        Public
    }
}
