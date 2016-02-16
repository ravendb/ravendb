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

        /// <summary>
        /// This is the tree number following the growth strategy for the tree structure. This virtual chunks
        /// are used to navigate the whole-tree in a cache concious fashion and are part of a virtual numbering of the nodes
        /// used for fast retrieval of node offsets.
        /// </summary>
        /// <remarks>
        /// While we would try to ensure multiple trees to share as much as possible chunks we cannot ensure 
        /// that is going to be the case without running a defrag operation. 
        /// </remarks>
        [FieldOffset(16)]
        public long Chunk;


    }
}
