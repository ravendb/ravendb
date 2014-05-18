using System;
using System.Runtime.InteropServices;
using Voron.Impl;

namespace Voron.Trees
{
	[StructLayout(LayoutKind.Explicit, Pack = 1)]
	public unsafe struct  NodeHeader
	{
		[FieldOffset(0)]
		public int DataSize;

		[FieldOffset(0)]
		public long PageNumber;

		[FieldOffset(8)]
		public NodeFlags Flags;

		[FieldOffset(9)]
		public ushort KeySize;

		[FieldOffset(11)]
		public ushort Version;

		public int GetNodeSize()
		{
			return Constants.NodeHeaderSize + KeySize + Constants.NodeOffsetSize + 
				  (Flags == (NodeFlags.PageRef) ? 0 : DataSize);
		}

		public static byte* DirectAccess(Transaction tx, NodeHeader* node)
		{
			if (node->Flags == (NodeFlags.PageRef))
			{
				var overFlowPage = tx.GetReadOnlyPage(node->PageNumber);
				return overFlowPage.Base + Constants.PageHeaderSize;
			}
			return (byte*) node + node->KeySize + Constants.NodeHeaderSize;
		}

        public static ValueReader Reader(Transaction tx, NodeHeader* node)
		{
			if (node->Flags == (NodeFlags.PageRef))
			{
				var overFlowPage = tx.GetReadOnlyPage(node->PageNumber);
                return new ValueReader(overFlowPage.Base + Constants.PageHeaderSize, overFlowPage.OverflowSize);
			}
            return new ValueReader((byte*)node + node->KeySize + Constants.NodeHeaderSize, node->DataSize);
		}

	    public static Slice GetData(Transaction tx, NodeHeader* node)
	    {
            if (node->Flags == (NodeFlags.PageRef))
            {
                var overFlowPage = tx.GetReadOnlyPage(node->PageNumber);
                if (overFlowPage.OverflowSize > ushort.MaxValue)
                    throw new InvalidOperationException("Cannot convert big data to a slice, too big");
                return new Slice(overFlowPage.Base + Constants.PageHeaderSize, (ushort)overFlowPage.OverflowSize);
            }
            return new Slice((byte*)node + node->KeySize + Constants.NodeHeaderSize, (ushort) node->DataSize);
	    }


        public static void CopyTo(Transaction tx, NodeHeader* node, byte* dest)
        {
            if (node->Flags == (NodeFlags.PageRef))
            {
                var overFlowPage = tx.GetReadOnlyPage(node->PageNumber);
                NativeMethods.memcpy(dest, overFlowPage.Base + Constants.PageHeaderSize, overFlowPage.OverflowSize);
            }
            NativeMethods.memcpy(dest, (byte*)node + node->KeySize + Constants.NodeHeaderSize, node->DataSize);
        }

		public static int GetDataSize(Transaction tx, NodeHeader* node)
		{
			if (node->Flags == (NodeFlags.PageRef))
			{
				var overFlowPage = tx.GetReadOnlyPage(node->PageNumber);
				return overFlowPage.OverflowSize;
			}
			return node->DataSize;
		}
	}
}