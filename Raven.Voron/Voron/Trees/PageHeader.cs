using System.Runtime.InteropServices;

namespace Voron.Trees
{
	[StructLayout(LayoutKind.Explicit, Pack = 1)]
	public struct PageHeader
	{
		[FieldOffset(0)]
        public long PageNumber;

		[FieldOffset(8)]
		public PageFlags Flags;

		[FieldOffset(9)]
		public ushort Lower;

		[FieldOffset(11)]
		public ushort Upper;

		[FieldOffset(13)]
		public int OverflowSize;
	}
}