using System.Runtime.InteropServices;

namespace Nevar
{
	[StructLayout(LayoutKind.Explicit, Pack = 1)]
	public struct PageHeader
	{
		[FieldOffset(0)]
		public int PageNumber;
		[FieldOffset(4)]
		public PageFlags Flags;

		[FieldOffset(5)]
		public ushort Lower;
		[FieldOffset(7)]
		public ushort Upper;

		[FieldOffset(5)]
		public int NumberOfPages;
	}
}