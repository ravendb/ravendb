using System;

namespace Voron.Trees
{
	[Flags]
	public enum PageFlags : byte
	{
		Branch = 1,
		Leaf = 2,
		Overflow = 4,
		KeysPrefixed = 8,
	}
}