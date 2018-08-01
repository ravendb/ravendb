namespace Raven.Client.Documents.Operations
{
    public enum OperationStatusFetchMode
    {
        /// <summary>
        /// Uses the Changes API to fetch the status
        /// </summary>
        ChangesApi,

        /// <summary>
        /// Uses simple HTTP polling to fetch the status. Suitable for systems that do not support Changes API capabilities like WebSockets
        /// </summary>
        Polling
    }
}
