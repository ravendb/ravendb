using System;
using System.Diagnostics;
using System.Text;
using Sparrow.Utils;

namespace Raven.Server.Documents.Replication
{
    public struct ChangeVectorEntry : IComparable<ChangeVectorEntry>
    {
        public Guid DbId;
        public long Etag;
        public int NodeTag;

        [ThreadStatic] private static char[] _threadBuffer;
        
        public unsafe void Append(StringBuilder sb)
        {
            ChangeVectorExtensions.ToBase26(sb, NodeTag);
            sb.Append(":");
            sb.Append(Etag);
            sb.Append("-");

            GuidToTruncatedBase64(sb, DbId);
        }

        public static unsafe void GuidToTruncatedBase64(StringBuilder sb, Guid id)
        {
            if (_threadBuffer == null)
                _threadBuffer = new char[24];
            fixed (char* buffer = _threadBuffer)
            {
                var result = Base64.ConvertToBase64Array(buffer, (byte*)&id, 0, 16);
                Debug.Assert(result == 24);
            }
            sb.Append(_threadBuffer, 0, 22);
        }
        
        public static unsafe string GuidToTruncatedBase64(Guid id)
        {
            if (_threadBuffer == null)
                _threadBuffer = new char[24];
            fixed (char* buffer = _threadBuffer)
            {
                var result = Base64.ConvertToBase64Array(buffer, (byte*)&id, 0, 16);
                Debug.Assert(result == 24);
            }
            return new string(_threadBuffer, 0, 22);
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
                return (DbId.GetHashCode() * 397) ^ Etag.GetHashCode();
            }
        }
        // we use it to sort change vectors by the ID.
        public int CompareTo(ChangeVectorEntry other)
        {
            var rc = DbId.CompareTo(other.DbId);
            if (rc != 0)
                return rc;
            return Etag.CompareTo(other.Etag);
        }
    }
}