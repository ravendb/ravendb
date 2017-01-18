using System;
using Sparrow.Json;

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

    public struct ChangeVectorEntry : IComparable
    {
        public Guid DbId;
        public long Etag;

        public bool Equals(ChangeVectorEntry other)
        {
            return DbId.Equals(other.DbId) && Etag == other.Etag;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (DbId.GetHashCode()*397) ^ Etag.GetHashCode();
            }
        }

        public int CompareTo(object obj)
        {
            return String.CompareOrdinal(DbId.ToString(), ((ChangeVectorEntry) obj).DbId.ToString());
        }
    }
}
