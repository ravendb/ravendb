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

        public long LastIndexTransformerEtagAccepted { get; set; }

        public string Error { get; set; }

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
