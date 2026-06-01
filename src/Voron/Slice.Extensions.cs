using System;
using System.Runtime.CompilerServices;
using Sparrow.Json;

namespace Voron
{
    public static class SliceExtensions
    {
        // Byte-faithful overload of JsonOperationContext.GetLazyString -- no JSON escape pass. Required
        // for binary slices (e.g. revision-tombstone composite PKs) whose bytes (0x1E RecordSeparator
        // among them) must reach the wire / consumer unchanged.
        public static unsafe LazyStringValue GetLazyString(this JsonOperationContext context, Slice slice, bool longLived = false)
            => context.GetLazyStringRaw(slice.Content.Ptr, slice.Size, longLived);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool StartWith(this Slice s1, ReadOnlySpan<byte> s2)
        {
            return s1.AsReadOnlySpan().StartsWith(s2);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]        
        public static bool EndsWith(this Slice s1, ReadOnlySpan<byte> s2)
        {
            return s1.AsReadOnlySpan().EndsWith(s2);
        }

        public static bool Contains(this ReadOnlySpan<byte> first, ReadOnlySpan<byte> second)
        {
            var length = first.Length - second.Length;
            if (length < 0)
                return false;

            // This is the last position with enough space to contain the other slice.             
            var end = length;

            byte firstByte = second[0];
            while (end >= 0)
            {
                if (first[end] == firstByte && second.SequenceCompareTo(first.Slice(end, second.Length)) == 0)
                    return true;

                end--;
            }

            return false;
        }
    }
}
