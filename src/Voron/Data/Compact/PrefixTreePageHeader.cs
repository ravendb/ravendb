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

        /// <summary>
        /// This is the relative index of the parent node at the parent page.
        /// </summary>
        [FieldOffset(14)]
        public int ParentNodeName;

        /// <summary>
        /// This is the virtual tree number for the parent tree. This number can be calculated from the VirtualPage
        /// but for performance reasons it makes sense to store it instead.
        /// </summary>
        [FieldOffset(18)]        
        public long ParentChunk;

        /// <summary>
        /// This is the root node name for the current tree in the whole-tree. This number can be calculated from the VirtualPage
        /// but for performance reasons it makes sense to store it instead.
        /// </summary>
        [FieldOffset(26)]
        public long RootNodeName;

        /// <summary>
        /// This is the tree number following the growth strategy for the tree structure. This virtual trees
        /// are used to navigate the whole-tree in a cache concious fashion and are part of a virtual numbering of the nodes
        /// used for fast retrieval of node offsets.
        /// </summary>
        [FieldOffset(34)]
        public long Chunk;
    }
}
