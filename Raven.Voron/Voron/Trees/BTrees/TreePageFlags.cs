namespace Voron.Trees
{
	using System;

	[Flags]
	public enum TreePageFlags : byte
	{
		None = 0,
		Branch = 1,
		Leaf = 2,
		Overflow = 4,
		KeysPrefixed = 8,
		FixedSize = 16
	}
}