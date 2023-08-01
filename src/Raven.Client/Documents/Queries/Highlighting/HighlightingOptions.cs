namespace Raven.Client.Documents.Queries.Highlighting
{
    public sealed class HighlightingOptions
    {
        public string GroupKey { get; set; }

        public string[] PreTags { get; set; }

        public string[] PostTags { get; set; }
    }
}

