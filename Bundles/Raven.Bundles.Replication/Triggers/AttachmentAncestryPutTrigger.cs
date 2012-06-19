//-----------------------------------------------------------------------
// <copyright file="AttachmentAncestryPutTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.ComponentModel.Composition;
using System.IO;
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
					var history = attachment.Metadata.Value<RavenJArray>(ReplicationConstants.RavenReplicationHistory) ??
					              new RavenJArray();
					metadata[ReplicationConstants.RavenReplicationHistory] = history;

					history.Add(new RavenJObject
					{
						{ReplicationConstants.RavenReplicationVersion, attachment.Metadata[ReplicationConstants.RavenReplicationVersion]},
						{ReplicationConstants.RavenReplicationSource, attachment.Metadata[ReplicationConstants.RavenReplicationSource]}

					});

					if (history.Length > ReplicationConstants.ChangeHistoryLength)
					{
						history.RemoveAt(0);
					}
				}
				metadata[ReplicationConstants.RavenReplicationVersion] = RavenJToken.FromObject(HiLo.NextId());
				metadata[ReplicationConstants.RavenReplicationSource] = RavenJToken.FromObject(Database.TransactionalStorage.Id);
			}
		}
	}
}