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
        public static ByteStringContext.ExternalAllocationScope ToSlicePtr(ByteStringContext context, TreeNodeHeader* node, out Slice slice)
        {
            return ToSlicePtr(context, node, ByteStringType.Mutable | (ByteStringType) SliceOptions.Key, out slice);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext.ExternalAllocationScope ToSlicePtr(ByteStringContext context, TreeNodeHeader* node, ByteStringType type, out Slice slice)
        {
            ByteString str;
            var scope = context.FromPtr((byte*)node + Constants.NodeHeaderSize, node->KeySize,
                type, out str);
            slice = new Slice(str);
            return scope;
        }

        public static byte* DirectAccess(LowLevelTransaction tx, TreeNodeHeader* node)
        {
            if (node->Flags == (TreeNodeFlags.PageRef))
            {
                var overFlowPage = tx.GetReadOnlyTreePage(node->PageNumber);
                return overFlowPage.Base + Constants.TreePageHeaderSize;
            }
            return (byte*) node + node->KeySize + Constants.NodeHeaderSize;
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

        public static ByteStringContext.ExternalAllocationScope GetData(LowLevelTransaction tx, TreeNodeHeader* node, out Slice slice)
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


        public static void CopyTo(LowLevelTransaction tx, TreeNodeHeader* node, byte* dest)
        {
            if (node->Flags == (TreeNodeFlags.PageRef))
            {
                var overFlowPage = tx.GetPage(node->PageNumber);
                Memory.Copy(dest, overFlowPage.Pointer + Constants.TreePageHeaderSize, overFlowPage.OverflowSize);
            }
            Memory.Copy(dest, (byte*)node + node->KeySize + Constants.NodeHeaderSize, node->DataSize);
        }

        public static int GetDataSize(LowLevelTransaction tx, TreeNodeHeader* node)
        {
            if (node->Flags == (TreeNodeFlags.PageRef))
            {
                var overFlowPage = tx.GetPage(node->PageNumber);
                return overFlowPage.OverflowSize;
            }
            return node->DataSize;
        }
    }
}