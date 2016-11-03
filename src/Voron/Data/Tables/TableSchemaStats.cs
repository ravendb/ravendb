using System.Runtime.InteropServices;

namespace Voron.Data.Tables
{
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public struct TableSchemaStats
    {
        [FieldOffset(0)]
        public long NumberOfEntries;

        [FieldOffset(8)]
        public long OverflowPageCount;
    }
}