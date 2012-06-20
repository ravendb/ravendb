//-----------------------------------------------------------------------
// <copyright file="VersioningPutTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Bundles.Versioning.Data;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.Versioning.Triggers
{
	public class VersioningPutTrigger : AbstractPutTrigger
	{
		public override VetoResult AllowPut(string key, RavenJObject document, RavenJObject metadata, TransactionInformation transactionInformation)
		{
			var jsonDocument = Database.Get(key, transactionInformation);
			if (jsonDocument == null)
				return VetoResult.Allowed;

			if (jsonDocument.Metadata.Value<string>(VersioningUtil.RavenDocumentRevisionStatus) == "Historical" && Database.IsVersioningActive(metadata))
			{
				return VetoResult.Deny("Modifying a historical revision is not allowed");
			}

			return VetoResult.Allowed;
		}

		public override void OnPut(string key, RavenJObject document, RavenJObject metadata, TransactionInformation transactionInformation)
		{
			if (key.StartsWith("Raven/", StringComparison.InvariantCultureIgnoreCase))
				return;

			if (metadata.Value<string>(VersioningUtil.RavenDocumentRevisionStatus) == "Historical")
				return;

			var versioningConfiguration = Database.GetDocumentVersioningConfiguration(metadata);
			if (versioningConfiguration == null || versioningConfiguration.Exclude)
				return;

			using (Database.DisableAllTriggersForCurrentThread())
			{
				var copyMetadata = new RavenJObject(metadata);
				copyMetadata[VersioningUtil.RavenDocumentRevisionStatus] = RavenJToken.FromObject("Historical");
				copyMetadata[Constants.RavenReadOnly] = true;
				copyMetadata.Remove(VersioningUtil.RavenDocumentRevision);
				var parentRevision = metadata.Value<string>(VersioningUtil.RavenDocumentRevision);
				if (parentRevision != null)
				{
					copyMetadata[VersioningUtil.RavenDocumentParentRevision] = key + "/revisions/" + parentRevision;
					metadata[VersioningUtil.RavenDocumentParentRevision] = key + "/revisions/" + parentRevision;
				}

				PutResult newDoc = Database.Put(key + "/revisions/", null, document, copyMetadata,
				                                transactionInformation);
				int revision = int.Parse(newDoc.Key.Split('/').Last());

				RemoveOldRevisions(key, revision, versioningConfiguration, transactionInformation);

				metadata[VersioningUtil.RavenDocumentRevisionStatus] = RavenJToken.FromObject("Current");
				metadata[VersioningUtil.RavenDocumentRevision] = RavenJToken.FromObject(revision);
			}
		}

		private void RemoveOldRevisions(string key, int revision, VersioningConfiguration versioningConfiguration, TransactionInformation transactionInformation)
		{
			int latestValidRevision = revision - versioningConfiguration.MaxRevisions;
			if (latestValidRevision <= 0)
				return;

			Database.Delete(string.Format("{0}/revisions/{1}", key, latestValidRevision), null, transactionInformation);
		}
	}
}