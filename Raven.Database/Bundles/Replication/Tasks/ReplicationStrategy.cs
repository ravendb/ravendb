using System;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Database;
using Raven.Database.Data;
using Raven.Json.Linq;

namespace Raven.Bundles.Replication.Tasks
{
	public class ReplicationStrategy
	{
		public bool FilterDocuments(string destinationId, string key, RavenJObject metadata)
		{
			if (IsSystemDocumentId(key)) 
				return false;
			if (metadata.ContainsKey(Constants.NotForReplication) && metadata.Value<bool>(Constants.NotForReplication)) // not explicitly marked to skip
				return false;
			if (metadata[Constants.RavenReplicationConflict] != null) // don't replicate conflicted documents, that just propagate the conflict
				return false;

			if (metadata.Value<string>(Constants.RavenReplicationSource) == destinationId) // prevent replicating back to source
				return false;

			switch (ReplicationOptionsBehavior)
			{
				case TransitiveReplicationOptions.None:
					var value = metadata.Value<string>(Constants.RavenReplicationSource);
					var replicateDoc = value == null || (value == CurrentDatabaseId);
					return replicateDoc;
			}
			return true;

		}

		public bool IsSystemDocumentId(string key)
		{
			if (key.StartsWith("Raven/", StringComparison.InvariantCultureIgnoreCase)) // don't replicate system docs
			{
				if (key.StartsWith("Raven/Hilo/", StringComparison.InvariantCultureIgnoreCase) == false) // except for hilo documents
					return true;
			}
			return false;
		}

		public bool FilterAttachments(AttachmentInformation attachment, string destinationInstanceId)
		{
			if (attachment.Key.StartsWith("Raven/", StringComparison.InvariantCultureIgnoreCase) || // don't replicate system attachments
			    attachment.Key.StartsWith("transactions/recoveryInformation", StringComparison.InvariantCultureIgnoreCase)) // don't replicate transaction recovery information
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