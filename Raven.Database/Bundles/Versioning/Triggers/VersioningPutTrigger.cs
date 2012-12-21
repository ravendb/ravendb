//-----------------------------------------------------------------------
// <copyright file="VersioningPutTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.ComponentModel.Composition;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Bundles.Versioning.Data;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.Versioning.Triggers
{
	[InheritedExport(typeof(AbstractPutTrigger))]
	[ExportMetadata("Bundle", "Versioning")]
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
			VersioningConfiguration versioningConfiguration;
			if (TryGetVersioningConfiguration(key, metadata, out versioningConfiguration) == false)
				return;

			int revision = 0;
			Database.TransactionalStorage.Batch(accessor =>
			{
				revision = (int)accessor.General.GetNextIdentityValue(key + "/revisions/");
			});
			using (Database.DisableAllTriggersForCurrentThread())
			{
				RemoveOldRevisions(key, revision, versioningConfiguration, transactionInformation);
			}
			metadata.__ExternalState["Next-Revision"] = revision;

			metadata.__ExternalState["Parent-Revision"] = metadata.Value<string>(VersioningUtil.RavenDocumentRevision);

			metadata[VersioningUtil.RavenDocumentRevisionStatus] = RavenJToken.FromObject("Current");
			metadata[VersioningUtil.RavenDocumentRevision] = RavenJToken.FromObject(revision);


		}

		public override void AfterPut(string key, RavenJObject document, RavenJObject metadata, Guid etag, TransactionInformation transactionInformation)
		{
			VersioningConfiguration versioningConfiguration;
			if (TryGetVersioningConfiguration(key, metadata, out versioningConfiguration) == false)
				return;

			using (Database.DisableAllTriggersForCurrentThread())
			{
				var copyMetadata = new RavenJObject(metadata);
				copyMetadata[VersioningUtil.RavenDocumentRevisionStatus] = RavenJToken.FromObject("Historical");
				copyMetadata[Constants.RavenReadOnly] = true;
				copyMetadata.Remove(VersioningUtil.RavenDocumentRevision);
				object parentRevision;
				metadata.__ExternalState.TryGetValue("Parent-Revision", out parentRevision);
				if (parentRevision != null)
				{
					copyMetadata[VersioningUtil.RavenDocumentParentRevision] = key + "/revisions/" + parentRevision;
					copyMetadata[VersioningUtil.RavenDocumentParentRevision] = key + "/revisions/" + parentRevision;
				}

				object value;
				metadata.__ExternalState.TryGetValue("Next-Revision", out value);
				Database.Put(key + "/revisions/" + value, null, (RavenJObject)document.CreateSnapshot(), copyMetadata,
							 transactionInformation);

			}
		}

		private bool TryGetVersioningConfiguration(string key, RavenJObject metadata,
												   out VersioningConfiguration versioningConfiguration)
		{
			versioningConfiguration = null;
			if (key.StartsWith("Raven/", StringComparison.InvariantCultureIgnoreCase))
				return false;

			if (metadata.Value<string>(VersioningUtil.RavenDocumentRevisionStatus) == "Historical")
				return false;

			versioningConfiguration = Database.GetDocumentVersioningConfiguration(metadata);
			if (versioningConfiguration == null || versioningConfiguration.Exclude)
				return false;
			return true;
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