using System;

namespace Raven.Bundles.Replication.Data
{
    public class SourceReplicationInformation
    {
        public Guid LastDocumentEtag { get; set; }
        public Guid LastAttachmentEtag { get; set; }

        public override string ToString()
        {
            return string.Format("LastDocumentEtag: {0}, LastAttachmentEtag: {1}", LastDocumentEtag, LastAttachmentEtag);
        }
    }
}
