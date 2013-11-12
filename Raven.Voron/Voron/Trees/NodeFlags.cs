namespace Voron.Trees
{
	public enum NodeFlags : byte
	{
		Data = 1,
		PageRef = 2,
        MultiValuePageRef = 3
	}
}