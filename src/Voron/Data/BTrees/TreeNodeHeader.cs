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
        public static ByteStringContext.InternalScope ToSlice(ByteStringContext context, TreeNodeHeader* node, out Slice str)
        {
            return ToSlice(context, node, ByteStringType.Immutable, out str);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext.InternalScope ToSlice(ByteStringContext context, TreeNodeHeader* node, ByteStringType type, out Slice str)
        {
            ByteString byteString;
            var scope = context.From((byte*) node + Constants.NodeHeaderSize, node->KeySize, type | (ByteStringType) SliceOptions.Key, out byteString);
            str = new Slice(byteString);
            return scope;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext.ExternalScope ToSlicePtr(ByteStringContext context, TreeNodeHeader* node, out Slice slice)
        {
            return ToSlicePtr(context, node, ByteStringType.Mutable | (ByteStringType) SliceOptions.Key, out slice);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext.ExternalScope ToSlicePtr(ByteStringContext context, TreeNodeHeader* node, ByteStringType type, out Slice slice)
        {
            ByteString str;
            var scope = context.FromPtr((byte*)node + Constants.NodeHeaderSize, node->KeySize,
                type, out str);
            slice = new Slice(str);
            return scope;
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

        public static ByteStringContext.ExternalScope GetData(LowLevelTransaction tx, TreeNodeHeader* node, out Slice slice)
        {
            if (node->Flags == (TreeNodeFlags.PageRef))
            {
                var overFlowPage = tx.GetPage(node->PageNumber);
                if (overFlowPage.OverflowSize > ushort.MaxValue)
                    throw new InvalidOperationException("Cannot convert big data to a slice, too big");
                return Slice.External(tx.Allocator, overFlowPage.Pointer + Constants.TreePageHeaderSize, (ushort)overFlowPage.OverflowSize, out slice);
            }
            return Slice.External(tx.Allocator, (byte*)node + node->KeySize + Constants.NodeHeaderSize, (ushort) node->DataSize, out slice);
        }
    }
}