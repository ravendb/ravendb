//-----------------------------------------------------------------------
// <copyright file="VersioningDeleteTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Bundles.Versioning.Data;
using Raven.Database.Plugins;
using Raven.Json.Linq;
using System.Linq;
using Raven.Abstractions.Extensions;

namespace Raven.Bundles.Versioning.Triggers
{
	[InheritedExport(typeof(AbstractDeleteTrigger))]
	[ExportMetadata("Bundle", "Versioning")]
	public class VersioningDeleteTrigger : AbstractDeleteTrigger
	{
	    readonly ThreadLocal<Dictionary<string, RavenJObject>> versionInformer 
			= new ThreadLocal<Dictionary<string, RavenJObject>>(() => new Dictionary<string, RavenJObject>());

		public override VetoResult AllowDelete(string key, TransactionInformation transactionInformation)
		{
			var document = Database.Get(key, transactionInformation);
			if (document == null)
				return VetoResult.Allowed;

			versionInformer.Value[key] = document.Metadata;
			if (document.Metadata.Value<string>(VersioningUtil.RavenDocumentRevisionStatus) != "Historical")
				return VetoResult.Allowed;

			if (Database.ChangesToRevisionsAllowed() == false &&
                Database.IsVersioningActive(document.Metadata))
			{
				var revisionPos = key.LastIndexOf("/revisions/", StringComparison.OrdinalIgnoreCase);
				if (revisionPos != -1)
				{
					var parentKey = key.Remove(revisionPos);
					var parentDoc = Database.Get(parentKey, transactionInformation);
					if (parentDoc == null)
						return VetoResult.Allowed;
				}

				return VetoResult.Deny("Deleting a historical revision is not allowed");
			}

			return VetoResult.Allowed;
		}

		public override void AfterDelete(string key, TransactionInformation transactionInformation)
		{
			var versioningConfig = Database.GetDocumentVersioningConfiguration(versionInformer.Value[key]);
	
			if (versioningConfig == null || !versioningConfig.PurgeOnDelete)
				return;

			Database.TransactionalStorage.Batch(accessor =>
			{
				while (true)
				{
					var revisionChildren = accessor.Documents.GetDocumentsWithIdStartingWith(key + "/revisions/", 0, 100).ToList();
					if (revisionChildren.Count == 0)
						break;

					foreach (var revisionChild in revisionChildren)
					{
						Database.Delete(revisionChild.Key, null, transactionInformation);
					}
				}
			});
		}
	}
}