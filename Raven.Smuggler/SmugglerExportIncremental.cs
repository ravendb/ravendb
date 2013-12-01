using Raven.Abstractions.Data;

namespace Raven.Smuggler
{
    public class SmugglerExportIncremental
    {
        public const string RavenDocumentKey = "Raven/Smuggler/Export/Incremental";
        public Etag LastDocsEtag { get; set; }
        public Etag LastAttachmentsEtag { get; set; }

        public SmugglerExportIncremental()
        {
            LastDocsEtag = Etag.Empty;
            LastAttachmentsEtag = Etag.Empty;
        }
    }
}