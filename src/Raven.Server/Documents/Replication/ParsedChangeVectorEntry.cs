using System;
using System.Runtime.CompilerServices;

namespace Raven.Server.Documents.Replication
{
    /// <summary>
    /// A change vector entry with its database id inline, so a buffer of these holds a parsed change vector without allocations
    /// (<see cref="ChangeVectorParser.ParseUtf8"/>). Ordered like <see cref="ChangeVectorEntry"/>: ordinal on the database id,
    /// then the etag.
    /// </summary>
    public struct ParsedChangeVectorEntry : IComparable<ParsedChangeVectorEntry>
    {
        public const int DbIdSize = 22;

        [InlineArray(DbIdSize)]
        public struct DbIdChars
        {
            private char _element;
        }

        public long Etag;
        public int NodeTag;
        public DbIdChars DbId;

        public readonly int CompareTo(ParsedChangeVectorEntry other)
        {
            ReadOnlySpan<char> dbId = DbId;
            var rc = dbId.SequenceCompareTo(other.DbId);
            return rc != 0 ? rc : Etag.CompareTo(other.Etag);
        }
    }
}
