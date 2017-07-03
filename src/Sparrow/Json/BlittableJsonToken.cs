using System;

namespace Sparrow.Json
{
    [Flags]
    public enum BlittableJsonToken : byte
    {
        StartObject = 1,
        StartArray = 2,
        Integer = 3,
        LazyNumber = 4,
        String = 5,
        CompressedString = 6,
        Boolean = 7,
        Null = 8,
        EmbeddedBlittable = 9,

        Reserved1 = 10,
        Reserved2 = 11,
        Reserved3 = 12,
        Reserved4 = 13,
        Reserved5 = 14,
        Reserved6 = 15,

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
