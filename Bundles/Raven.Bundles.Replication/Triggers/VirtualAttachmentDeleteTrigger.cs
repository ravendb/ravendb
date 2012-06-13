//-----------------------------------------------------------------------
// <copyright file="VirtualAttachmentDeleteTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.ComponentModel.Composition;
using System.IO;
using System.Threading;
using Raven.Bundles.Replication.Impl;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.Replication.Triggers
{
	/// <summary>
	/// We can't allow real deletes when using replication, because
	/// then we won't have any way to replicate the delete. Instead
	/// we allow the delete but don't do actual delete, we replace it 
	/// with a delete marker instead
	/// </summary>
	[ExportMetadata("Bundle", "Replication")]
	[ExportMetadata("Order", 10000)]
	[InheritedExport(typeof(AbstractAttachmentDeleteTrigger))]
	public class VirtualAttachmentDeleteTrigger : AbstractAttachmentDeleteTrigger
	{
		readonly ThreadLocal<RavenJArray> deletedHistory = new ThreadLocal<RavenJArray>();
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

		public override void OnDelete(string key)
		{
			using(Database.DisableAllTriggersForCurrentThread())
			{
				var attachment = Database.GetStatic(key);
				if (attachment == null)
					return;
				deletedHistory.Value = attachment.Metadata.Value<RavenJArray>(ReplicationConstants.RavenReplicationHistory) ??
									   new RavenJArray();

				deletedHistory.Value.Add(
					new RavenJObject
				{
					{ReplicationConstants.RavenReplicationVersion, attachment.Metadata[ReplicationConstants.RavenReplicationVersion]},
					{ReplicationConstants.RavenReplicationSource, attachment.Metadata[ReplicationConstants.RavenReplicationSource]}
				});
			}
		}

		public override void AfterDelete(string key)
		{
			using(Database.DisableAllTriggersForCurrentThread())
			{
				var metadata = new RavenJObject
				{
					{"Raven-Delete-Marker", true},
					{ReplicationConstants.RavenReplicationHistory, deletedHistory.Value},
					{ReplicationConstants.RavenReplicationSource, Database.TransactionalStorage.Id.ToString()},
					{ReplicationConstants.RavenReplicationVersion, HiLo.NextId()}
				};
				deletedHistory.Value = null;
				Database.PutStatic(key, null, new MemoryStream(new byte[0]), metadata);
			}
		}
	}
}