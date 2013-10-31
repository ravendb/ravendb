using System;
using System.IO;
using System.Runtime.InteropServices;
using Voron.Impl;

namespace Voron.Trees
{
	using System.Collections.Generic;
	using System.Linq;

	using Voron.Util;

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

		[FieldOffset(11)]
		public ushort Version;

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

				var streams = new List<Stream>();
				while (overFlowPage.NextOverflowPage != -1)
				{
					streams.Add(new UnmanagedMemoryStream(overFlowPage.Base + Constants.PageHeaderSize, overFlowPage.OverflowSize,
												 overFlowPage.OverflowSize, FileAccess.Read));

					overFlowPage = tx.GetReadOnlyPage(overFlowPage.NextOverflowPage);
				}

				streams.Add(new UnmanagedMemoryStream(overFlowPage.Base + Constants.PageHeaderSize, overFlowPage.OverflowSize,
												 overFlowPage.OverflowSize, FileAccess.Read));

				return new OverflowStream(streams);
			}

			return new UnmanagedMemoryStream((byte*)node + node->KeySize + Constants.NodeHeaderSize, node->DataSize,
											 node->DataSize, FileAccess.Read);
		}

		public unsafe static Slice GetData(Transaction tx, NodeHeader* node)
		{
			if (node->Flags == (NodeFlags.PageRef))
			{
				var overFlowPage = tx.GetReadOnlyPage(node->PageNumber);
				if (overFlowPage.OverflowSize > ushort.MaxValue)
					throw new InvalidOperationException("Cannot convert big data to a slice, too big");
				return new Slice(overFlowPage.Base + Constants.PageHeaderSize, (ushort)overFlowPage.OverflowSize);
			}
			return new Slice((byte*)node + node->KeySize + Constants.NodeHeaderSize, (ushort)node->DataSize);
		}


		public unsafe static void CopyTo(Transaction tx, NodeHeader* node, byte* dest)
		{
			if (node->Flags == (NodeFlags.PageRef))
			{
				var overFlowPage = tx.GetReadOnlyPage(node->PageNumber);
				NativeMethods.memcpy(dest, overFlowPage.Base + Constants.PageHeaderSize, overFlowPage.OverflowSize);
			}
			NativeMethods.memcpy(dest, (byte*)node + node->KeySize + Constants.NodeHeaderSize, node->DataSize);
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