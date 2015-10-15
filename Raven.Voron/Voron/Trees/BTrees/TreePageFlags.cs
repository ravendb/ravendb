namespace Voron.Trees
{
	using System;

    [Flags]
    public enum PageFlags : byte
    {
        Single = 0,
        Overflow = 1
    }

	[Flags]
	public enum TreePageFlags : byte
	{
		None = 0,
		Branch = 1,
		Leaf = 2,
		Value = 4,
		Unused = 8,
		FixedSize = 16
	}
}