using System.IO;
using System.Runtime.InteropServices;
using Nevar.Impl;

namespace Nevar.Trees
{
	[StructLayout(LayoutKind.Explicit, Pack = 1)]
	public struct NodeHeader
	{
		[FieldOffset(0)]
		public int DataSize;
		[FieldOffset(0)]
		public long PageNumber;

		[FieldOffset(8)]
		public NodeFlags Flags;

		[FieldOffset(9)]
		public ushort KeySize;

		public int GetNodeSize()
		{
			return Constants.NodeHeaderSize + KeySize + Constants.NodeOffsetSize + 
				  (Flags == (NodeFlags.PageRef) ? Constants.PageNumberSize : DataSize);
		}


		public unsafe static Stream Stream(Transaction tx, NodeHeader* node)
		{
			if (node->Flags == (NodeFlags.PageRef))
			{
				var overFlowPage = tx.GetReadOnlyPage(node->PageNumber);
				return new UnmanagedMemoryStream(overFlowPage.Base + Constants.PageHeaderSize, overFlowPage.OverflowSize,
												 overFlowPage.OverflowSize, FileAccess.Read);
			}
			return new UnmanagedMemoryStream((byte*)node + node->KeySize + Constants.NodeHeaderSize, node->DataSize,
											 node->DataSize, FileAccess.Read);
		}

		public unsafe static int GetDataSize(Transaction tx, NodeHeader* node)
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