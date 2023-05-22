using System.Runtime.InteropServices;

namespace Voron.Util.Simd;

[StructLayout(LayoutKind.Explicit, Pack = 1)]
public struct SimdBitPackingHeader
{
    [FieldOffset(0)]
    public long Baseline;
    [FieldOffset(8)]
    public ushort Prefix;
    [FieldOffset(10)]
    public ushort OffsetToMetadata;
    [FieldOffset(12)]
    public byte ShiftAmount;
    [FieldOffset(13)]
    public byte NumberOfFullSegments;
    [FieldOffset(14)]
    public byte LastSegmentCount;
    [FieldOffset(15)]
    private readonly byte Reserved;
}
