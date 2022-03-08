using System;
using System.Text;
using Sparrow;

namespace Raven.Server.Documents.Replication
{
    public struct ChangeVectorEntry : IComparable<ChangeVectorEntry>, IEquatable<ChangeVectorEntry>
    {
        public string DbId;
        public long Etag;
        public int NodeTag;

        public void Append(StringBuilder sb)
        {
            ChangeVectorExtensions.ToBase26(sb, NodeTag);
            sb.Append(":");
            sb.Append(Etag);
            sb.Append("-");
            sb.Append(DbId);
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            Append(sb);
            return sb.ToString();
        }

        public bool Equals(ChangeVectorEntry other)
        {
            return DbId.Equals(other.DbId) && Etag == other.Etag;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((int)Hashing.Marvin32.Calculate(DbId) * 397) ^ Etag.GetHashCode();
            }
        }
        // we use it to sort change vectors by the ID.
        public int CompareTo(ChangeVectorEntry other)
        {
            var rc = string.Compare(DbId, other.DbId, StringComparison.Ordinal);
            if (rc != 0)
                return rc;
            return Etag.CompareTo(other.Etag);
        }

        public static implicit operator ChangeVectorEntry((string dbId, long etag, int nodeTag) entry)
        {
            return new ChangeVectorEntry
            {
                DbId = entry.dbId,
                Etag = entry.etag,
                NodeTag = entry.nodeTag
            };
        }     
    }
}
