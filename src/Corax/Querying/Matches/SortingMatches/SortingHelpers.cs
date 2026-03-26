using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Sparrow;

namespace Corax.Querying.Matches.SortingMatches;

internal static class SortingHelpers
{
    public const long InvalidTermId = -1;
    
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
