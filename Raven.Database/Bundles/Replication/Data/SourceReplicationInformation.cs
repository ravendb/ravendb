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

		public Guid ServerInstanceId { get; set; }

		public string Source { get; set; }

		public DateTime? LastModified { get; set; }

		public int? LastBatchSize { get; set; }

		public string SourceCollections { get; set; }

		public override string ToString()
		{
			return string.Format("LastDocumentEtag: {0}", LastDocumentEtag);
		}

		public SourceReplicationInformation()
		{
			LastDocumentEtag = Etag.Empty;
		}
	}

	public class SourceReplicationInformationWithBatchInformation : SourceReplicationInformation
	{
		public int? MaxNumberOfItemsToReceiveInSingleBatch { get; set; }
	}
}