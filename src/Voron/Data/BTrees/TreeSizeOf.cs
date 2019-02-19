using System.Runtime.CompilerServices;
using Voron.Data.BTrees;
using Voron.Global;

namespace Voron.Impl
{
    internal unsafe class TreeSizeOf
    {
        /// <summary>
        /// Calculate the size of a leaf node.
        /// The size depends on the environment's page size; if a data item
        /// is too large it will be put onto an overflow page and the node
        /// size will only include the key and not the data. Sizes are always
        /// rounded up to an even number of bytes, to guarantee 2-byte alignment
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int LeafEntry(int pageMaxSpace, Slice key, int len)
        {
            var nodeSize = Constants.Tree.NodeHeaderSize;

            if (key.Options == SliceOptions.Key)
                nodeSize += key.Size;

            if (len != 0)
            {
                nodeSize += len;

                if (nodeSize > pageMaxSpace)
                    nodeSize -= len - Constants.Tree.PageNumberSize;
            }
            // else - page ref node, take no additional space

            nodeSize += nodeSize & 1;

            return nodeSize;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int BranchEntry(Slice key)
        {
            var sz = Constants.Tree.NodeHeaderSize + key.Size;
            sz += sz & 1;
            return sz;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int NodeEntry(int pageMaxSpace, Slice key, int len)
        {
            if (len < 0)
                return BranchEntry(key);

            return LeafEntry(pageMaxSpace, key, len);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int NodeEntry(TreeNodeHeader* other)
        {
            var sz = other->KeySize + Constants.Tree.NodeHeaderSize;
            if (other->Flags == TreeNodeFlags.Data || other->Flags == TreeNodeFlags.MultiValuePageRef)
                sz += other->DataSize;

            sz += sz & 1;

            return sz;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int NodeEntryWithAnotherKey(TreeNodeHeader* other, Slice key)
        {
            var keySize = key.HasValue ? key.Size : other->KeySize;
            var sz = keySize + Constants.Tree.NodeHeaderSize;
            if (other->Flags == TreeNodeFlags.Data || other->Flags == TreeNodeFlags.MultiValuePageRef)
                sz += other->DataSize;

            sz += sz & 1;

            return sz;
        }
    }
}
