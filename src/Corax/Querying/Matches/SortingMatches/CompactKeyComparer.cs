using System;
using Sparrow;

namespace Corax.Querying.Matches.SortingMatches;

public static unsafe class CompactKeyComparer
{
    public static int Compare(UnmanagedSpan xItem, UnmanagedSpan yItem, int nullResult)
    {
        if (yItem.Address == null)
        {
            if (xItem.Address == null)
                return 0;

            return nullResult;
        }

        if (xItem.Address == null)
            return -nullResult;
        
        var match = Memory.Compare(xItem.Address + 1, yItem.Address + 1, Math.Min(xItem.Length - 1, yItem.Length - 1));
        if (match != 0)
            return match;

        var xItemLengthInBits = (xItem.Length - 1) * 8 - (xItem.Address[0] >> 4);
        var yItemLengthInBits = (yItem.Length - 1) * 8 - (yItem.Address[0] >> 4);
        return xItemLengthInBits - yItemLengthInBits;
    }
}
