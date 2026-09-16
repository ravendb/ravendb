namespace Voron.Data.RoaringBitmaps;

public enum ContainerType : byte
{
    /// <summary>Sorted ushort array. Binary search for Contains. Merge-based set ops.</summary>
    Array = 0,
    /// <summary>8KB bitmap (1024 longs). Direct bit access.</summary>
    Bitmap = 1,
    /// <summary>
    /// Contiguous values RangeStart..RangeStart+Cardinality-1 are set. No data allocation needed.
    /// Cardinality == BitsPerContainer means all 65,536 bits set (full container).
    /// Sequential Add at either edge is an O(1) increment.
    /// </summary>
    Range = 2,
    /// <summary>
    /// Unsorted ushort array. Add is O(1) append. On the first read (Contains, set ops, iteration),
    /// sorts and deduplicates, converting to Array. Avoids O(log n + shift) per Add.
    /// </summary>
    ArrayUnsorted = 3,
    /// <summary>Tombstone marker for free-list entries in the entries array.</summary>
    Free = 0xFF
}
