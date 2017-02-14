namespace Raven.Client.Documents.Replication.Messages
{
    internal class TopologyDiscoveryResponseHeader
    {
        public Status Type;

        public string Exception;

        public string Message;

        internal enum Status
        {
            AlreadyKnown = 1,
            Ok = 2,
            Error = 3
        }
    }
}
