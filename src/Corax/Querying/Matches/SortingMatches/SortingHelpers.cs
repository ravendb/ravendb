using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Corax.Querying.Matches.Meta;
using Sparrow;
using Sparrow.Server;
using Sparrow.Server.Utils;

namespace Corax.Querying.Matches.SortingMatches;

internal static class SortingHelpers
{
    public const long InvalidTermId = -1;

    /// <summary>Drains match into an allocator-backed buffer; the caller is responsible for disposing the scope once done.</summary>
    public static unsafe ByteStringContext<ByteStringMemoryCache>.InternalScope DrainMatch<TInner>(
        ref TInner inner, ByteStringContext allocator, out Span<long> results)
        where TInner : IQueryMatch
    {
        var count = inner.Count;
        int bufferSize = count is > 0 and < (1024 * 1024) ? (int)count : 4096;
        var scope = allocator.Allocate(bufferSize * sizeof(long), out var bs);
        var buffer = new Span<long>(bs.Ptr, bufferSize);
        int filled = 0;
        int r;
        while ((r = inner.Fill(buffer[filled..])) > 0)
        {
            filled += r;
            if (filled < buffer.Length) 
                continue;
            
            allocator.GrowAllocation(ref bs, ref scope, buffer.Length * sizeof(long));
            buffer = new Span<long>(bs.Ptr, bs.Length / sizeof(long));
        }

        int unique = Sorting.SortAndRemoveDuplicates(buffer[..filled]);
        results = buffer[..unique];
        
        return scope;
    }
    
    /// <summary>
    /// There are textual values for fields that are either null or do not exist. However, since we want to specifically control the order of the nulls,
    /// we need to rewrite them and put them inside a specific "bucket". Since we do not want to compare literals all the time, we will replace them with an UnmanagedSpan where the address is a null pointer.
    /// </summary>
    public const long MissingTermId = long.MinValue;
    
    
    public static void ReplaceNullAndNonExistingTermIds(Span<long> buffer, long nonExistingTermId, long nullTermId, long replaceWith)
    {
        if (nonExistingTermId == InvalidTermId && nullTermId == InvalidTermId)
            return;
        
        int idX = 0;
        ref var bufferRef = ref MemoryMarshal.GetReference(buffer);
        if (AdvInstructionSet.IsAcceleratedVector512)
        {
            var N = Vector512<long>.Count;
            var nonExistingVector = Vector512.Create(nonExistingTermId);
            var nullVector = Vector512.Create(nullTermId);
            var replaceWithVector = Vector512.Create(replaceWith);
            for (; idX + N <= buffer.Length; idX += N)
            {
                var currentMask = Vector512.LoadUnsafe(ref Unsafe.Add(ref bufferRef, idX));
                var isNull = Vector512.Equals(currentMask, nullVector);
                var isNonExisting = Vector512.Equals(currentMask, nonExistingVector);
                var combinedMask = isNull | isNonExisting;
                var result = Vector512.ConditionalSelect(combinedMask, replaceWithVector, currentMask);
                result.StoreUnsafe(ref Unsafe.Add(ref bufferRef, idX));
            }
                
        }
            
        for (; idX < buffer.Length; idX++)
        {
            if (buffer[idX] == nonExistingTermId || buffer[idX] == nullTermId)
                buffer[idX] = replaceWith;
        }
    }
}
