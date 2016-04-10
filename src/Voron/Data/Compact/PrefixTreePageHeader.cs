using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace Voron.Data.Compact
{
    [StructLayout(LayoutKind.Explicit, Size = 42, Pack = 1)]
    public struct PrefixTreePageHeader
    {
        /// <summary>
        /// The physical storage page number
        /// </summary>
        [FieldOffset(0)]
        public long PageNumber;

        [FieldOffset(8)]
        public int OverflowSize;

        [FieldOffset(12)]
        public PageFlags Flags;

        [FieldOffset(14)]
        public ushort NodesPerChunk;
    }
}
