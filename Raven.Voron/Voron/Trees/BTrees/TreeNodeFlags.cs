namespace Voron.Trees
{
	public enum TreeNodeFlags : byte
	{
		Data = 1,
		PageRef = 2,
        MultiValuePageRef = 3
	}
}