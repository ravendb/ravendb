//-----------------------------------------------------------------------
// <copyright file="SourceReplicationInformation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Bundles.Replication.Data
{
    public class SourceReplicationInformation
    {
        public Etag LastDocumentEtag { get; set; }

        [Obsolete("Use RavenFS instead.")]
        public Etag LastAttachmentEtag { get; set; }

        public Guid ServerInstanceId { get; set; }

        public string Source { get; set; }

        public DateTime? LastModified { get; set; }

        public int? LastBatchSize { get; set; }

        public string SourceCollections { get; set; }

        [JsonIgnore]
        public bool IsETL => string.IsNullOrEmpty(SourceCollections) == false;

        public override string ToString()
        {
            return string.Format("LastDocumentEtag: {0}, LastAttachmentEtag: {1}", LastDocumentEtag, LastAttachmentEtag);
        }

        public SourceReplicationInformation()
        {
            LastDocumentEtag = Etag.Empty;
            LastAttachmentEtag = Etag.Empty;
        }
    }

    public class SourceReplicationInformationWithBatchInformation : SourceReplicationInformation
    {
        public int? MaxNumberOfItemsToReceiveInSingleBatch { get; set; }

        public Guid? DatabaseId { get; set; }
    }
}
