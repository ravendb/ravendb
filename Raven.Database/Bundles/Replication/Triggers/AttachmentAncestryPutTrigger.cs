//-----------------------------------------------------------------------
// <copyright file="AttachmentAncestryPutTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.ComponentModel.Composition;
using System.IO;
using Raven.Abstractions.Data;
using Raven.Bundles.Replication.Impl;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.Replication.Triggers
{
	[ExportMetadata("Bundle", "Replication")]
	[ExportMetadata("Order", 10000)]
	[InheritedExport(typeof(AbstractAttachmentPutTrigger))]
	public class AttachmentAncestryPutTrigger : AbstractAttachmentPutTrigger
	{
		internal ReplicationHiLo HiLo
		{
			get
			{
				return (ReplicationHiLo)Database.ExtensionsState.GetOrAdd(typeof(ReplicationHiLo), o => new ReplicationHiLo
				{
					Database = Database
				});
			}
		}

		public override void OnPut(string key, Stream data, RavenJObject metadata)
		{
			if (key.StartsWith("Raven/")) // we don't deal with system attachment
				return;
			using (Database.DisableAllTriggersForCurrentThread())
			{
				var attachment = Database.GetStatic(key);
				if (attachment != null)
				{
					var history = attachment.Metadata.Value<RavenJArray>(Constants.RavenReplicationHistory) ??
					              new RavenJArray();
					metadata[Constants.RavenReplicationHistory] = history;

					history.Add(new RavenJObject
					{
						{Constants.RavenReplicationVersion, attachment.Metadata[Constants.RavenReplicationVersion]},
						{Constants.RavenReplicationSource, attachment.Metadata[Constants.RavenReplicationSource]}

					});

					if (history.Length > Constants.ChangeHistoryLength)
					{
						history.RemoveAt(0);
					}
				}
				metadata[Constants.RavenReplicationVersion] = RavenJToken.FromObject(HiLo.NextId());
				metadata[Constants.RavenReplicationSource] = RavenJToken.FromObject(Database.TransactionalStorage.Id);
			}
		}
	}
}