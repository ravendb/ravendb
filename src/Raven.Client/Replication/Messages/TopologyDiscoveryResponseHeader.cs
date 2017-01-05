namespace Raven.Client.Replication.Messages
{
    public class TopologyDiscoveryResponseHeader
    {
        public Status Type;

        public string Exception;

        public string Message;

        public enum Status
        {
            AlreadyKnown = 1,
            Ok = 2,
            Error = 3
        }
    }
}
