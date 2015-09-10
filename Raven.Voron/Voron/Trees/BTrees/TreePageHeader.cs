using System.Runtime.InteropServices;

namespace Voron.Trees
{
	[StructLayout(LayoutKind.Explicit, Pack = 1)]
	public struct TreePageHeader
	{
		[FieldOffset(0)]
		public long PageNumber;

		[FieldOffset(8)]
		public TreePageFlags Flags;

		[FieldOffset(9)]
		public ushort Lower;

		[FieldOffset(9)]
		public ushort FixedSize_ValueSize;

		[FieldOffset(11)]
		public ushort Upper;

		[FieldOffset(11)]
		public ushort FixedSize_NumberOfEntries;


		[FieldOffset(13)]
		public int OverflowSize;

		[FieldOffset(13)]
		public ushort FixedSize_StartPosition;
	}

}