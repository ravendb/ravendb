//-----------------------------------------------------------------------
// <copyright file="SourceReplicationInformation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;

namespace Raven.Bundles.Replication.Data
{
	public class SourceReplicationInformation
	{
		public Guid LastDocumentEtag { get; set; }
		public Guid LastAttachmentEtag { get; set; }
		public Guid ServerInstanceId { get; set; }

		public override string ToString()
		{
			return string.Format("LastDocumentEtag: {0}, LastAttachmentEtag: {1}", LastDocumentEtag, LastAttachmentEtag);
		}
	}
}