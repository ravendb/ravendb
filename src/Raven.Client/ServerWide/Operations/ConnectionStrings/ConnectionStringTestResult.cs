namespace Raven.Client.ServerWide.Operations.ConnectionStrings
{
    /// <summary>
    /// The result of a server-wide connection-string test operation.
    /// </summary>
    public sealed class ConnectionStringTestResult
    {
        /// <summary>
        /// Indicates whether the test connection succeeded.
        /// </summary>
        public bool Success { get; set; }

        /// <summary>
        /// The error message, populated when <see cref="Success"/> is <c>false</c>.
        /// </summary>
        public string Error { get; set; }

        /// <summary>
        /// The TCP server URL, populated for some providers (e.g. ElasticSearch) on success.
        /// </summary>
        public string TcpServerUrl { get; set; }
    }
}
