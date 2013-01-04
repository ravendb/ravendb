using ActiproSoftware.Text.Tagging;

namespace Raven.Studio.Features.JsonEditor
{
    public enum LinkTagNavigationType
    {
        Document,
        ExternalUrl,
    }

    public class LinkTag : ITag
    {
        public LinkTagNavigationType NavigationType { get; set; }
        public string Url { get; set; }
    }
}