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

        public string Exception { get; set; }

        public string Message { get; set; }

        public string MessageType { get; set; }

        public ChangeVectorEntry[] DocumentsChangeVector { get; set; }

        public ChangeVectorEntry[] IndexTransformerChangeVector { get; set; }

        public string DatabaseId { get; set; }
    }

    public struct ChangeVectorEntry
    {
        public Guid DbId;
        public long Etag;

        public override bool Equals(object other)
        {
            if (!(other is ChangeVectorEntry))
            {
                return false;
            }
            ChangeVectorEntry changeVector = (ChangeVectorEntry) other;
            return DbId.Equals(changeVector.DbId) && Etag == changeVector.Etag;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (DbId.GetHashCode()*397) ^ Etag.GetHashCode();
            }
        }
    }
}
