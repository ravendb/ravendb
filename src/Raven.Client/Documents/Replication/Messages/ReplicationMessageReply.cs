using System;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Replication.Messages
{
    internal class ReplicationMessageReply
    {
        internal enum ReplyType
        {
            None,
            Ok,
            Error
        }

        public ReplyType Type { get; set; }
        public long LastEtagAccepted { get; set; }
        public string Exception { get; set; }
        public string Message { get; set; }
        public string MessageType { get; set; }
        public ChangeVectorEntry[] ChangeVector { get; set; }
        public string DatabaseId { get; set; }
    }
    public struct ChangeVectorEntry : IComparable<ChangeVectorEntry>
    {
        public Guid DbId;
        public long Etag;
        public int NodeTag;
        public void Append(StringBuilder sb)
        {
            ChangeVectorExtensions.ToBase26(sb, NodeTag);
            sb.Append(":");
            sb.Append(Etag);
            sb.Append("-");
            //TODO: Fix this allocation mess
            sb.Append(Convert.ToBase64String(DbId.ToByteArray()));
        }
        // TODO: don't use regex
        private static readonly Regex Regex = new Regex(@"(?<node>\w+):(?<etag>\d+)-(?<dbId>.+)", RegexOptions.Compiled);
        public static ChangeVectorEntry FromString(string str)
        {
            var match = Regex.Match(str);
            if (match == null)
            {
                throw new InvalidDataException($"Unable to parse the change vector string {str}.");
            }
            var groups = match.Groups;
            return new ChangeVectorEntry
            {
                DbId = new Guid(Convert.FromBase64String(groups["dbId"].Value)),
                Etag = Int32.Parse(groups["etag"].Value),
                NodeTag = ChangeVectorExtensions.FromBase26(groups["node"].Value)
            };
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