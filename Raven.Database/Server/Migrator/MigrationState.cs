using System;
using Raven.Abstractions.Data;

namespace Raven.Database.Server.Migrator
{
    public class MigrationState
    {
        public Etag LastDocumentEtag { get; set; }
        public Etag LastDocumentDeleteEtag { get; set; }

        [Obsolete("Use RavenFS instead.")]
        public Etag LastAttachmentEtag { get; set; }

        [Obsolete("Use RavenFS instead.")]
        public Etag LastAttachmentDeleteEtag { get; set; }
    }
}