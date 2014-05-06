namespace Raven.Tests.Common.Dto.TagCloud
{
	public class TagAndCount
	{
		public string Tag { get; set; }
		public long Count { get; set; }

		public override string ToString()
		{
			return string.Format("Tag: {0}, Count: {1}", Tag, Count);
		}
	}
}