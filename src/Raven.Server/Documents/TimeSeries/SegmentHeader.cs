using System.Runtime.InteropServices;

namespace Raven.Server.Documents.TimeSeries
{
    [StructLayout(LayoutKind.Explicit, Size = 16)]
    public unsafe struct SegmentHeader
    {
        [FieldOffset(0)]
        public int PreviousTimestamp;
        [FieldOffset(4)]
        public int PreviousDelta;
        [FieldOffset(8)]
        public ushort NumberOfEntries;
        [FieldOffset(10)]
        public ushort SizeOfTags;
        [FieldOffset(12)]
        public byte PreviousTagIndex;
        [FieldOffset(13)]
        public byte NumberOfValues;
        [FieldOffset(14)]
        public SegmentVersion Version;
        [FieldOffset(15)]
        public fixed byte Reserved[1];
    }

    [StructLayout(LayoutKind.Explicit, Size = 1)]
    public struct SegmentVersion
    {
        [FieldOffset(0)]
        public Version Number;

        public static SegmentVersion Create() => new SegmentVersion(Version.Baseline);

        private SegmentVersion(Version number)
        {
            Number = number;
        }

        public bool ContainsDuplicates => (Number & Version.Duplicates) != 0;
        public void SetDuplicates() => Number |= Version.Duplicates;

        public void SetLastValueDuplicate() => Number |= (Version.LastDuplicate | Version.Duplicates);
        public void ClearLastValueDuplicate() => Number &= ~Version.LastDuplicate;
        public bool ContainsLastValueDuplicate => (Number & Version.LastDuplicate) != 0;
    }

    public enum Version : byte
    {
        V50000 = 0,
        V50001 = 1,
        Baseline = V50001,

        // last 2 bits treated as flags
        Duplicates = 0b0100_0000, // segment contain duplicates
        LastDuplicate = 0b1000_0000 // last value in the segment is a duplicate
    }
}
