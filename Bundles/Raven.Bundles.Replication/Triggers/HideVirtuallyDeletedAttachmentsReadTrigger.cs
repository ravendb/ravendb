//-----------------------------------------------------------------------
// <copyright file="HideVirtuallyDeletedAttachmentsReadTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Newtonsoft.Json.Linq;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.Replication.Triggers
{
    public class HideVirtuallyDeletedAttachmentsReadTrigger : AbstractAttachmentReadTrigger
    {
		public override ReadVetoResult AllowRead(string key, byte[] data, RavenJObject metadata, ReadOperation operation)
        {
            if (ReplicationContext.IsInReplicationContext)
                return ReadVetoResult.Allowed;
            RavenJToken value;
            if (metadata.TryGetValue("Raven-Delete-Marker", out value))
                return ReadVetoResult.Ignore;
            return ReadVetoResult.Allowed;
        }
    }
}