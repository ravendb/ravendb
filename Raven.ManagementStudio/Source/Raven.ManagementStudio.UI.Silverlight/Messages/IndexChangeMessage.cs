namespace Raven.ManagementStudio.UI.Silverlight.Messages
{
	using Indexes.Browse;

	public class IndexChangeMessage
    {
        public IndexViewModel Index { get; set; }

        public bool IsRemoved { get; set; }
    }
}
