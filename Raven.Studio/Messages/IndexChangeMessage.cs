namespace Raven.Studio.Messages
{
	using Features.Indexes;

	public class IndexChangeMessage
	{
		public EditIndexViewModel Index { get; set; }

		public bool IsRemoved { get; set; }
	}
}