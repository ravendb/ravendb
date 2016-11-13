using System;

namespace Raven.Client.Replication.Messages
{
    public class ReplicationMessageReply
    {
        public enum ReplyType
        {
            None,
            Ok,
            Error
        }

        public ReplyType Type { get; set; }

        public long LastEtagAccepted { get; set; }

        public string Error { get; set; }

        public string MessageType { get; set; }

        public ChangeVectorEntry[] CurrentChangeVector { get; set; }
    }

    public struct ChangeVectorEntry
    {
        public Guid DbId;
        public long Etag;
    }
}
