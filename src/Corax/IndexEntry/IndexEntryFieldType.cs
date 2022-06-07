using System;

namespace Corax;

[Flags]
public enum IndexEntryFieldType : ushort
{
    Null = 0,
    Simple = 1,

    Tuple = 1 << 1,
    List = 1 << 2,
    Raw = 1 << 3,
    SpatialPoint = 1 << 5,

    Empty = 1 << 13,
    HasNulls = 1 << 14, // Helper for list writer.
    Invalid = 1 << 15,
    
    ListWithNulls = List | HasNulls,
    TupleListWithNulls = TupleList | HasNulls,
    SpatialPointList = List | SpatialPoint,
    TupleList = List | Tuple,
    RawList = List | Raw
}
