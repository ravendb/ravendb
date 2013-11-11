namespace Voron.Trees
{
	public enum PageFlags : byte
	{
		Branch = 1,
		Leaf = 2,
		Overflow = 3,
		Meta = 4,
	}
}