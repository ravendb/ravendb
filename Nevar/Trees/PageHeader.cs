using System.Runtime.InteropServices;

namespace Nevar.Trees
{
	[StructLayout(LayoutKind.Explicit, Pack = 1)]
	public struct PageHeader
	{
		[FieldOffset(0)]
        public long PageNumber;

		[FieldOffset(8)]
		public PageFlags Flags;

		[FieldOffset(10)]
		public ushort Lower;

		[FieldOffset(12)]
		public ushort Upper;

		[FieldOffset(14)]
		public int OverflowSize;
	}
}