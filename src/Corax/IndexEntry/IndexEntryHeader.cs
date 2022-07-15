using System.Runtime.InteropServices;

namespace Corax.IndexEntry;

[StructLayout(LayoutKind.Explicit, Size = HeaderSize)]
internal struct IndexEntryHeader
{
    // Format for the header: [length: uint][known_field_count:ushort][dynamic_table_offset:uint]
    internal const int LengthOffset = 0; // 0b
    internal const int KnownFieldCountOffset = sizeof(uint); // 4b 
    internal const int MetadataTableOffset = KnownFieldCountOffset + sizeof(ushort); // 4b + 2b = 6b
    internal const int HeaderSize = MetadataTableOffset + sizeof(uint); // 4b + 2b + 4b = 10b

    [FieldOffset(LengthOffset)]
    public uint Length;

    // The known field count is encoded as xxxxxxyy where:
    // x: the count
    // y: the encode size
    [FieldOffset(KnownFieldCountOffset)]
    public ushort KnownFieldCount;

    [FieldOffset(MetadataTableOffset)]
    public uint DynamicTable;
}
