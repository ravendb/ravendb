//-----------------------------------------------------------------------
// <copyright file="VersioningDeleteTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.ComponentModel.Composition;
using Raven.Abstractions.Data;
using Raven.Database.Plugins;

namespace Raven.Bundles.Versioning.Triggers
{
	[InheritedExport(typeof(AbstractDeleteTrigger))]
	[ExportMetadata("Bundle", "Versioning")]
	public class VersioningDeleteTrigger : AbstractDeleteTrigger
	{
		public override VetoResult AllowDelete(string key, TransactionInformation transactionInformation)
		{
			JsonDocument document = Database.Get(key, transactionInformation);
			if (document == null)
				return VetoResult.Allowed;
			if (document.Metadata.Value<string>(VersioningUtil.RavenDocumentRevisionStatus) != "Historical")
				return VetoResult.Allowed;

			if (Database.IsVersioningActive(document.Metadata))
				return VetoResult.Deny("Deleting a historical revision is not allowed");

			return VetoResult.Allowed;
		}
	}
}