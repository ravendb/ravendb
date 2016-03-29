using System;

namespace Sparrow.Json
{
    [Flags]
    public enum BlittableJsonToken : byte
    {
        StartObject = 1,
        StartArray = 2,
        Integer = 3,
        Float = 4,
        String = 5,
        CompressedString = 6,
        Boolean = 7,
        Null = 8,

        // Position sizes 
        OffsetSizeByte = 16,
        OffsetSizeShort = 32,
        OffsetSizeInt = 48,

        // PropertyId sizes
        PropertyIdSizeByte = 64,
        PropertyIdSizeShort = 128,
        PropertyIdSizeInt = 192
    }
}