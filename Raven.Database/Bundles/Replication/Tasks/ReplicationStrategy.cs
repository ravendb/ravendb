using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Replication;
using Raven.Database.Data;
using Raven.Json.Linq;

namespace Raven.Bundles.Replication.Tasks
{
    public class ReplicationStrategy
    {
        private static readonly ILog Log = LogManager.GetCurrentClassLogger();

        public bool FilterDocuments(string destinationId, string key, RavenJObject metadata, out string reason)
        {
            if (IsSystemDocumentId(key))
            {
                reason = string.Format("Will not replicate document '{0}' to '{1}' because it is a system document", key, destinationId);
                if (Log.IsDebugEnabled)
                    Log.Debug(reason);
                return false;
            }

            if (metadata.ContainsKey(Constants.NotForReplication) && metadata.Value<bool>(Constants.NotForReplication))
                // not explicitly marked to skip
            {
                reason = string.Format("Will not replicate document '{0}' to '{1}' because it was marked as not for replication", key, destinationId);
                if (Log.IsDebugEnabled)
                    Log.Debug(reason); 
                return false;
            }

            if (metadata[Constants.RavenReplicationConflict] != null)
                // don't replicate conflicted documents, that just propagate the conflict
            {
                reason = string.Format("Will not replicate document '{0}' to '{1}' because it a conflict document", key, destinationId);
                if (Log.IsDebugEnabled)
                    Log.Debug(reason); 
                return false;
            }

            if (OriginsFromDestination(destinationId, metadata)) // prevent replicating back to source
            {
                reason = string.Format("Will not replicate document '{0}' to '{1}' because the destination server is the same server it originated from", key, destinationId);
                if (Log.IsDebugEnabled)
                    Log.Debug(reason); 
                return false;
            }
            
            switch (ReplicationOptionsBehavior)
            {
                case TransitiveReplicationOptions.None:
                    var value = metadata.Value<string>(Constants.RavenReplicationSource);
                    if (value != null &&  (value != CurrentDatabaseId))
                    {
                        reason = string.Format("Will not replicate document '{0}' to '{1}' because it was not created on the current server, and TransitiveReplicationOptions = none", key, destinationId);
                        if (Log.IsDebugEnabled)
                            Log.Debug(reason);
                        return false;
                    }
                    break;
            }

            reason = string.Format("Will replicate '{0}' to '{1}'", key, destinationId);
            if (Log.IsDebugEnabled)
                Log.Debug(reason);

            return true;
        }

        public bool OriginsFromDestination(string destinationId, RavenJObject metadata)
        {
            return metadata.Value<string>(Constants.RavenReplicationSource) == destinationId;
        }

        public bool OriginatedAtOtherDestinations(string sourceId, RavenJObject metadata)
        {
            return metadata.Value<string>(Constants.RavenReplicationSource) != sourceId;
        }

        public bool IsSystemDocumentId(string key)
        {
            if (key.StartsWith("Raven/", StringComparison.OrdinalIgnoreCase)) // don't replicate system docs
            {
                if (key.StartsWith("Raven/Hilo/", StringComparison.OrdinalIgnoreCase) == false) // except for hilo documents
                    return true;
            }

            return false;
        }

        [Obsolete("Use RavenFS instead.")]
        public bool FilterAttachments(AttachmentInformation attachment, string destinationInstanceId)
        {
            if (attachment.Key.StartsWith("Raven/", StringComparison.OrdinalIgnoreCase) || // don't replicate system attachments
                attachment.Key.StartsWith("transactions/recoveryInformation", StringComparison.OrdinalIgnoreCase)) // don't replicate transaction recovery information
                return false;

            // explicitly marked to skip
            if (attachment.Metadata.ContainsKey(Constants.NotForReplication) && attachment.Metadata.Value<bool>(Constants.NotForReplication))
                return false;

            if (attachment.Metadata.ContainsKey(Constants.RavenReplicationConflict))// don't replicate conflicted documents, that just propagate the conflict
                return false;

            // we don't replicate stuff that was created there
            if (attachment.Metadata.Value<string>(Constants.RavenReplicationSource) == destinationInstanceId)
                return false;

            switch (ReplicationOptionsBehavior)
            {
                case TransitiveReplicationOptions.None:
                    return attachment.Metadata.Value<string>(Constants.RavenReplicationSource) == null ||
                           (attachment.Metadata.Value<string>(Constants.RavenReplicationSource) == CurrentDatabaseId);
            }

            return true;
        }

        public Dictionary<string, string> SpecifiedCollections { get; set; }

        public string CurrentDatabaseId { get; set; }

        public TransitiveReplicationOptions ReplicationOptionsBehavior { get; set; }
        public RavenConnectionStringOptions ConnectionStringOptions { get; set; }

        public override string ToString()
        {
            return string.Join(" ", new[]
            {
                ConnectionStringOptions.Url,
                ConnectionStringOptions.DefaultDatabase,
                ConnectionStringOptions.ApiKey
            }.Where(x => x != null));
        }

    }
}
