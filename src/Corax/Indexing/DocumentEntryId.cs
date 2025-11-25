using System;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;

namespace Corax.Indexing
{
    /// <summary>
    /// Strongly-typed document entry identifier. Prevents confusion with ContainerEntryId (storage layer)
    /// since both were represented as raw longs. The type system now catches ID misuse at compile time,
    /// eliminating bugs like deduplication failures, duplicate processing, corruption, and wrong query results.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = sizeof(long))]
    public readonly struct DocumentEntryId : IEquatable<DocumentEntryId>, IComparable<DocumentEntryId>
    {
        [FieldOffset(0)]
        private readonly long _id;

        public DocumentEntryId(long id)
        {
            _id = id;
        }

        public static readonly DocumentEntryId Invalid = new(-1);

        public bool IsValid => _id > 0;
        public bool IsEmpty => _id == 0;

        public static explicit operator long(DocumentEntryId entryId) => entryId._id;
        public static explicit operator DocumentEntryId(long id) => new(id);

        public bool Equals(DocumentEntryId other) => _id == other._id;
        public override bool Equals(object obj) => obj is DocumentEntryId other && Equals(other);
        public override int GetHashCode() => _id.GetHashCode();
        public override string ToString() => $"{nameof(DocumentEntryId)}({_id})";

        public int CompareTo(DocumentEntryId other) => _id.CompareTo(other._id);

        public static bool operator ==(DocumentEntryId left, DocumentEntryId right) => left._id == right._id;
        public static bool operator !=(DocumentEntryId left, DocumentEntryId right) => left._id != right._id;
        public static bool operator <(DocumentEntryId left, DocumentEntryId right) => left._id < right._id;
        public static bool operator >(DocumentEntryId left, DocumentEntryId right) => left._id > right._id;
        public static bool operator <=(DocumentEntryId left, DocumentEntryId right) => left._id <= right._id;
        public static bool operator >=(DocumentEntryId left, DocumentEntryId right) => left._id >= right._id;

        /// <summary>
        /// Reinterprets a Span of DocumentEntryId as a Span of long for performance-critical operations like sorting.
        /// Safe because DocumentEntryId is explicitly laid out with Size = sizeof(long) and a single long field at offset 0.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Span<long> AsLongSpan(Span<DocumentEntryId> span)
        {
            return MemoryMarshal.Cast<DocumentEntryId, long>(span);
        }

        /// <summary>
        /// Reinterprets a ReadOnlySpan of DocumentEntryId as a ReadOnlySpan of long.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ReadOnlySpan<long> AsLongSpan(ReadOnlySpan<DocumentEntryId> span)
        {
            return MemoryMarshal.Cast<DocumentEntryId, long>(span);
        }
    }
}
