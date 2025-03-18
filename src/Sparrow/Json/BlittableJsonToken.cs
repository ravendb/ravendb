using System;

namespace Sparrow.Json
{
    //note: There are overlapping bits in the values,
    // so for some values HasFlag() can return invalid results.
    // This is by design, so bit packing can be done
    [Flags]
    public enum BlittableJsonToken : byte
    {
        StartObject = 0x01,
        StartArray = 0x02,
        Integer = 0x03,
        LazyNumber = 0x04,
        String = 0x05,
        CompressedString = 0x06,
        Boolean = 0x07,
        Null = 0x08,
        EmbeddedBlittable = 0x09,
        RawBlob = 0x0A,
        Vector = 0x0B,

        Reserved3 = 0x0C,
        Reserved4 = 0x0D,
        Reserved5 = 0x0E,
        Reserved6 = 0x0F,

        // Position sizes 
        OffsetSizeByte = 0b0001_0000,
        OffsetSizeShort = 0b0010_0000,
        OffsetSizeInt =   0b0011_0000,

        // PropertyId sizes
        PropertyIdSizeByte =  0b0100_0000,
        PropertyIdSizeShort = 0b1000_0000,
        PropertyIdSizeInt = 0b1100_0000,

        TypesMask = OffsetSizeByte - 1, // 0b0000_1111
        PositionMask = OffsetSizeByte | OffsetSizeShort | OffsetSizeInt, // 0b0011_0000       
        PropertyIdMask = PropertyIdSizeByte | PropertyIdSizeShort | PropertyIdSizeInt,  // 0b1100_0000    
    }
}
