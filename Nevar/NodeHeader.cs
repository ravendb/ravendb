using System.Runtime.InteropServices;

namespace Nevar
{
	[StructLayout(LayoutKind.Explicit, Pack = 1)]
	public struct NodeHeader
	{
		[FieldOffset(0)]
		public int DataSize;
		[FieldOffset(0)]
		public int PageNumber;

		[FieldOffset(4)]
		public NodeFlags Flags;

		[FieldOffset(6)]
		public ushort KeySize;

		public int GetNodeSize()
		{
			return Constants.NodeHeaderSize + KeySize + Constants.NodeOffsetSize + 
				  (Flags.HasFlag(NodeFlags.PageRef) ? Constants.PageNumberSize : DataSize);
		}
	}

	[StructLayout(LayoutKind.Explicit, Pack = 1)]
	public unsafe struct NodeDebug
	{
		[FieldOffset(0)]
		public int DataSize;
		[FieldOffset(0)]
		public int PageNumber;

		[FieldOffset(4)]
		public NodeFlags Flags;

		[FieldOffset(6)]
		public ushort KeySize;

		[FieldOffset(8)]
		public byte* Data;

		public override string ToString()
		{
			return new Slice(Data, KeySize).ToString();
		}
	}
}