//-----------------------------------------------------------------------
// <copyright file="VersioningPutTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.ComponentModel.Composition;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
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

            if (Database.ChangesToRevisionsAllowed() == false && 
                jsonDocument.Metadata.Value<string>(VersioningUtil.RavenDocumentRevisionStatus) == "Historical" &&
                Database.IsVersioningActive(metadata))
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

			var revision = GetNextRevisionNumber(key);

			using (Database.DisableAllTriggersForCurrentThread())
			{
				RemoveOldRevisions(key, revision, versioningConfiguration, transactionInformation);
			}
			metadata.__ExternalState["Next-Revision"] = revision;

			metadata.__ExternalState["Parent-Revision"] = metadata.Value<string>(VersioningUtil.RavenDocumentRevision);

			metadata[VersioningUtil.RavenDocumentRevisionStatus] = RavenJToken.FromObject("Current");
			metadata[VersioningUtil.RavenDocumentRevision] = RavenJToken.FromObject(revision);
		}

		public override void AfterPut(string key, RavenJObject document, RavenJObject metadata, Etag etag, TransactionInformation transactionInformation)
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

		private int GetNextRevisionNumber(string key)
		{
			var revision = 0;

			Database.TransactionalStorage.Batch(accessor =>
			{
				revision = (int)accessor.General.GetNextIdentityValue(key + "/revisions/");

				if (revision == 1)
				{
					var existingDoc = Database.Get(key, null);
					if (existingDoc != null)
					{
						RavenJToken existingRevisionToken;
						if (existingDoc.Metadata.TryGetValue(VersioningUtil.RavenDocumentRevision, out existingRevisionToken))
							revision = existingRevisionToken.Value<int>() + 1;
					}
					else
					{
						var latestRevisionsDoc = GetLatestRevisionsDoc(key);
						if (latestRevisionsDoc != null)
						{
							var id = latestRevisionsDoc["@metadata"].Value<string>("@id");
							if(id.StartsWith(key, StringComparison.CurrentCultureIgnoreCase))
							{
								var revisionNum = id.Substring((key + "/revisions/").Length);
								int result;
								if (int.TryParse(revisionNum, out result))
									revision = result + 1;
							}
							
						}
					}

					if (revision > 1)
						accessor.General.SetIdentityValue(key + "/revisions/", revision);
				}
			});

			return revision;
		}

		private RavenJObject GetLatestRevisionsDoc(string key)
		{
			const int pageSize = 100;
			int start = 0;

			RavenJObject lastRevisionDoc = null;

			while (true)
			{
				var docs = Database.GetDocumentsWithIdStartingWith(key + "/revisions/", null, null, start, pageSize);
				if (!docs.Any())
					break;

				lastRevisionDoc = (RavenJObject)docs.Last();

				if (docs.Length < pageSize)
					break;

				start += pageSize;
			}

			return lastRevisionDoc;
		}

		private bool TryGetVersioningConfiguration(string key, RavenJObject metadata,
												   out VersioningConfiguration versioningConfiguration)
		{
			versioningConfiguration = null;
			if (key.StartsWith("Raven/", StringComparison.OrdinalIgnoreCase))
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