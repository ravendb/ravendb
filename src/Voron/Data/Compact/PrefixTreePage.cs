using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Voron.Data.Compact
{
    public sealed unsafe class PrefixTreePage
    {
        public readonly byte* Pointer;
        public readonly int PageSize;
        public readonly string Source;

        private PrefixTreePageHeader* Header
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (PrefixTreePageHeader*)Pointer; }
        }

        public byte* DataPointer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Pointer + sizeof(PrefixTreePageHeader); }
        }

        public PrefixTreePage(byte* pointer, string source, int pageSize)
        {
            Pointer = pointer;
            Source = source;
            PageSize = pageSize;            
        }

        public long PageNumber
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Header->PageNumber; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { Header->PageNumber = value; }
        }

        /// <summary>
        /// This is the relative index of the parent node at the parent page.
        /// </summary>
        public int ParentNodeName
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Header->ParentNodeName; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { Header->ParentNodeName = value; }
        }


        /// <summary>
        /// This is the virtual tree number for the parent tree. This number can be calculated from the VirtualPage
        /// but for performance reasons it makes sense to store it instead.
        /// </summary>
        public long ParentChunk
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Header->ParentChunk; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { Header->ParentChunk = value; }
        }

        /// <summary>
        /// This is the tree number following the growth strategy for the tree structure. This virtual trees
        /// are used to navigate the whole-tree in a cache concious fashion and are part of a virtual numbering of the nodes
        /// used for fast retrieval of node offsets.
        /// </summary>
        public long Chunk
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Header->Chunk; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { Header->Chunk = value; }
        }

        /// <summary>
        /// This is the root node name for the current tree in the whole-tree. This number can be calculated from the VirtualPage
        /// but for performance reasons it makes sense to store it instead.
        /// </summary>
        public long RootNodeName
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Header->RootNodeName; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { Header->RootNodeName = value; }
        }


        public override string ToString()
        {
            return $"#{PageNumber}";
        }

        /// <summary>
        /// Will return the offset from the <see cref="DataPointer"/> to the Node.
        /// </summary>
        /// <remarks>This method will not do any bound check</remarks>
        /// <param name="nodeNameInTree">the relative name of the node in the tree.</param>
        /// <returns>the offset in the page memory for that node</returns>
        public static long GetNodeOffset(long nodeNameInTree)
        {            
            // TODO: Check if the formula is correct. 
            return nodeNameInTree * sizeof(PrefixTree.Node);
        }

        /// <summary>
        /// Will return a pointer to the actual node in the tree
        /// </summary>
        /// <param name="nodeNameInTree">the relative name of the node in the tree.</param>
        /// <returns>the node for that node</returns>
        public PrefixTree.Node* GetNodeByName(long nodeNameInTree)
        {            
            return (PrefixTree.Node*)(DataPointer + sizeof(PrefixTree.Node) * nodeNameInTree);
            throw new NotImplementedException();
        }
    }
}
