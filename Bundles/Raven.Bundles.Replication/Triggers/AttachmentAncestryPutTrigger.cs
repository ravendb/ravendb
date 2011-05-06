//-----------------------------------------------------------------------
// <copyright file="AttachmentAncestryPutTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.ComponentModel.Composition;
using Newtonsoft.Json.Linq;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.Replication.Triggers
{
	[ExportMetadata("Order", 10000)]
	public class AttachmentAncestryPutTrigger : AbstractAttachmentPutTrigger
    {
        private ReplicationHiLo hiLo;
        public override void Initialize()
        {
            base.Initialize();
            hiLo = new ReplicationHiLo
            {
                Database = Database
            };
        }

		public override void OnPut(string key, byte[] data, RavenJObject metadata)
        {
            if (key.StartsWith("Raven/")) // we don't deal with system attachment
                return;
            var attachment = Database.GetStatic(key);
            if (attachment != null)
            {
            	var history = metadata.Value<RavenJArray>(ReplicationConstants.RavenReplicationHistory);
				if (history == null)
					metadata[ReplicationConstants.RavenReplicationHistory] = history = new RavenJArray();

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
			metadata[ReplicationConstants.RavenReplicationVersion] = RavenJToken.FromObject(hiLo.NextId());
            metadata[ReplicationConstants.RavenReplicationSource] = RavenJToken.FromObject(Database.TransactionalStorage.Id);
        }
    }
}