using System;

namespace Nevar.Trees
{
	[Flags]
	public enum PageFlags : byte
	{
		None = 0x00,
		Branch = 0x01,
		Leaf = 0x02,
		Overlfow = 0x04,
		Meta = 0x08,
	}
}