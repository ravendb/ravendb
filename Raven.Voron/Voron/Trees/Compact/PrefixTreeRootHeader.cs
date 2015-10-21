using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace Voron.Trees.Compact
{
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public struct PrefixTreeRootHeader
    {
        [FieldOffset(0)]
        public long RootPageNumber;
        [FieldOffset(8)]
        public long BranchPages;
        [FieldOffset(16)]
        public long LeafPages;
        [FieldOffset(40)]
        public long PageCount;
        [FieldOffset(48)]
        public long NumberOfEntries;
        [FieldOffset(56)]
        public int Depth;
        [FieldOffset(60)]
        public PrefixTreeFlags Flags;
    }
}
