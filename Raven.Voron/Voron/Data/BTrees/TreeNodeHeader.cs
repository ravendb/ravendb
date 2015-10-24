using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow;
using Voron.Impl;

namespace Voron.Data.BTrees
{
	[StructLayout(LayoutKind.Explicit, Pack = 1)]
	public unsafe struct  TreeNodeHeader
	{
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
			return Constants.NodeHeaderSize + KeySize + Constants.NodeOffsetSize + (Flags == (TreeNodeFlags.PageRef) ? 0 : DataSize);
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

				Debug.Assert(overFlowPage.IsOverflow, "Requested oveflow page but got " + overFlowPage.Flags);
				Debug.Assert(overFlowPage.OverflowSize > 0, "Overflow page cannot be size equal 0 bytes");

                return new ValueReader(overFlowPage.Pointer + Constants.TreePageHeaderSize, overFlowPage.OverflowSize);
			}
            return new ValueReader((byte*)node + node->KeySize + Constants.NodeHeaderSize, node->DataSize);
		}

	    public static Slice GetData(LowLevelTransaction tx, TreeNodeHeader* node)
	    {
            if (node->Flags == (TreeNodeFlags.PageRef))
            {
                var overFlowPage = tx.GetPage(node->PageNumber);
                if (overFlowPage.OverflowSize > ushort.MaxValue)
                    throw new InvalidOperationException("Cannot convert big data to a slice, too big");
                return new Slice(overFlowPage.Pointer + Constants.TreePageHeaderSize, (ushort)overFlowPage.OverflowSize);
            }
            return new Slice((byte*)node + node->KeySize + Constants.NodeHeaderSize, (ushort) node->DataSize);
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