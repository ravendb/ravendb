//-----------------------------------------------------------------------
// <copyright file="HideVirtuallyDeletedAttachmentsReadTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.ComponentModel.Composition;
using Newtonsoft.Json.Linq;
using Raven.Database.Plugins;

namespace Raven.Bundles.Replication.Triggers
{
	[ExportMetadata("Order", 10000)]
	public class HideVirtuallyDeletedAttachmentsReadTrigger : AbstractAttachmentReadTrigger
    {
        public override ReadVetoResult AllowRead(string key, byte[] data, JObject metadata, ReadOperation operation)
        {
            if (ReplicationContext.IsInReplicationContext)
                return ReadVetoResult.Allowed;
            JToken value;
            if (metadata.TryGetValue("Raven-Delete-Marker", out value))
                return ReadVetoResult.Ignore;
            return ReadVetoResult.Allowed;
        }
    }
}