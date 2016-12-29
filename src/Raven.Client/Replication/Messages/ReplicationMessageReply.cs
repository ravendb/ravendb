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

        public string DbId { get; set; }

        public ReplyType Type { get; set; }

        public long LastEtagAccepted { get; set; }

        public long LastIndexTransformerEtagAccepted { get; set; }

        public string Exception { get; set; }

        public string MessageType { get; set; }

        public ChangeVectorEntry[] DocumentsChangeVector { get; set; }

        public ChangeVectorEntry[] IndexTransformerChangeVector { get; set; }
    }

    public struct ChangeVectorEntry
    {
        public Guid DbId;
        public long Etag;
    }
}
