using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow;
using Sparrow.Server;
using Voron.Impl;
using Constants = Voron.Global.Constants;

namespace Voron.Data.BTrees
{
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public unsafe struct TreeNodeHeader
    {
        public const int SizeOf = 11;

        private const int NodeTypeSizeOf = Constants.Tree.NodeHeaderSize + Constants.Tree.NodeOffsetSize;

        [FieldOffset(0)]
        public int DataSize;

        [FieldOffset(0)]
        public long PageNumber;

        [FieldOffset(8)]
        public TreeNodeFlags Flags;

        [FieldOffset(9)]
        public ushort KeySize;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetNodeSize()
        {
            return NodeTypeSizeOf + KeySize  + (Flags == (TreeNodeFlags.PageRef) ? 0 : DataSize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string ToDebugString(TreeNodeHeader* node)
        {
            return Encodings.Utf8.GetString((byte*)node + Constants.Tree.NodeHeaderSize, node->KeySize);
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
            var scope = context.From((byte*)node + Constants.Tree.NodeHeaderSize, node->KeySize, type | (ByteStringType) SliceOptions.Key, out byteString);
            str = new Slice(byteString);
            return scope;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext.ExternalScope ToSlicePtr(ByteStringContext context, TreeNodeHeader* node, out Slice slice)
        {
            ByteString str;
            var scope = context.FromPtr((byte*)node + Constants.Tree.NodeHeaderSize, node->KeySize, ByteStringType.Mutable | (ByteStringType)SliceOptions.Key, out str);
            slice = new Slice(str);
            return scope;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext.ExternalScope ToSlicePtr(ByteStringContext context, TreeNodeHeader* node, ByteStringType type, out Slice slice)
        {
            ByteString str;
            var scope = context.FromPtr((byte*)node + Constants.Tree.NodeHeaderSize, node->KeySize, type, out str);
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

                return new ValueReader(overFlowPage.Pointer + Constants.Tree.PageHeaderSize, overFlowPage.OverflowSize);
            }
            return new ValueReader((byte*)node + node->KeySize + Constants.Tree.NodeHeaderSize, node->DataSize);
        }

        public static ByteStringContext.ExternalScope GetData(LowLevelTransaction tx, TreeNodeHeader* node, out Slice slice)
        {
            if (node->Flags == (TreeNodeFlags.PageRef))
            {
                var overFlowPage = tx.GetPage(node->PageNumber);
                if (overFlowPage.OverflowSize > ushort.MaxValue)
                    throw new InvalidOperationException("Cannot convert big data to a slice, too big");
                return Slice.External(tx.Allocator, overFlowPage.Pointer + Constants.Tree.PageHeaderSize, (ushort)overFlowPage.OverflowSize, out slice);
            }
            return Slice.External(tx.Allocator, (byte*)node + node->KeySize + Constants.Tree.NodeHeaderSize, (ushort) node->DataSize, out slice);
        }
    }
}