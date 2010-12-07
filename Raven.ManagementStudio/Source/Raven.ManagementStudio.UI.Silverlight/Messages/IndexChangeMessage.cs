using Raven.ManagementStudio.UI.Silverlight.Plugins.Indexes.Browse;

namespace Raven.ManagementStudio.UI.Silverlight.Messages
{
    public class IndexChangeMessage
    {
        public IndexViewModel Index { get; set; }

        public bool IsRemoved { get; set; }
    }
}
