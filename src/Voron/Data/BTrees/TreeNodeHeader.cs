using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Sparrow;
using Voron.Global;
using Voron.Impl;

namespace Voron.Data.BTrees
{
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public unsafe struct TreeNodeHeader
    {
        static readonly int _nodeTypeSize = Constants.NodeHeaderSize + Constants.NodeOffsetSize;

        [FieldOffset(0)]
        public int DataSize;

        [FieldOffset(0)]
        public long PageNumber;

        [FieldOffset(8)]
        public TreeNodeFlags Flags;

        [FieldOffset(9)]
        public ushort KeySize;

        [FieldOffset(11)]
        public ushort Version;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetNodeSize()
        {
            return _nodeTypeSize + KeySize  + (Flags == (TreeNodeFlags.PageRef) ? 0 : DataSize);
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string ToDebugString(TreeNodeHeader* node)
        {
            return Encoding.UTF8.GetString((byte*)node + Constants.NodeHeaderSize, node->KeySize);
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Slice ToSlice(ByteStringContext context, TreeNodeHeader* node, ByteStringType type = ByteStringType.Mutable)
        {
            return new Slice(context.From((byte*)node + Constants.NodeHeaderSize, node->KeySize, type | (ByteStringType)SliceOptions.Key));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Slice ToSlicePtr(ByteStringContext context, TreeNodeHeader* node, ByteStringType type = ByteStringType.Mutable)
        {
            return new Slice(context.FromPtr((byte*)node + Constants.NodeHeaderSize, node->KeySize, type | (ByteStringType)SliceOptions.Key));
        }

        public static ValueReader Reader(LowLevelTransaction tx, TreeNodeHeader* node)
        {
            if (node->Flags == (TreeNodeFlags.PageRef))
            {
                var overFlowPage = tx.GetPage(node->PageNumber);

                Debug.Assert(overFlowPage.IsOverflow, "Requested overflow page but got " + overFlowPage.Flags);
                Debug.Assert(overFlowPage.OverflowSize > 0, "Overflow page cannot be size equal 0 bytes");

                return new ValueReader(overFlowPage.Pointer + Constants.TreePageHeaderSize, overFlowPage.OverflowSize);
            }
            return new ValueReader((byte*)node + node->KeySize + Constants.NodeHeaderSize, node->DataSize);
        }
    }
}