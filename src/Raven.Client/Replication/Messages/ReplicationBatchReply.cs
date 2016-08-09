using System;

namespace Raven.Abstractions.Replication
{
    public class ReplicationBatchReply
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

        public ChangeVectorEntry[] CurrentChangeVector { get; set; }
    }

    public struct ChangeVectorEntry
    {
        public Guid DbId;
        public long Etag;
    }
}
