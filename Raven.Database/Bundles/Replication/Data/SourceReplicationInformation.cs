//-----------------------------------------------------------------------
// <copyright file="SourceReplicationInformation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;

namespace Raven.Bundles.Replication.Data
{
	public class SourceReplicationInformation
	{
		public Etag LastDocumentEtag { get; set; }
		public Etag LastAttachmentEtag { get; set; }
		public Guid ServerInstanceId { get; set; }
		public string Source { get; set; }

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
}