using Newtonsoft.Json.Linq;
using Raven.Database.Plugins;

namespace Raven.Bundles.Replication.Triggers
{
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