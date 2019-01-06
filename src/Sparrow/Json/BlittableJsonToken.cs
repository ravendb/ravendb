using System;

namespace Sparrow.Json
{
    //note: There are overlapping bits in the values,
    // so for some values HasFlag() can return invalid results.
    // This is by design, so bit packing can be done
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
        RawBlob = 10,

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
