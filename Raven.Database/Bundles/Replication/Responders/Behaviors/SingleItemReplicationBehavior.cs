using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database;
using Raven.Database.Storage;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Bundles.Replication.Responders
{
	using Raven.Database.Bundles.Replication.Impl;

	public abstract class SingleItemReplicationBehavior<TInternal, TExternal>
	{
		private readonly ILog log = LogManager.GetCurrentClassLogger();

		public DocumentDatabase Database { get; set; }
		public IStorageActionsAccessor Actions { get; set; }
		public string Src { get; set; }

		public void Replicate(string id, RavenJObject metadata, TExternal incoming)
		{
			if(metadata.Value<bool>(Constants.RavenDeleteMarker))
			{
				ReplicateDelete(id, metadata, incoming);
				return;
			}
			TInternal existingItem;
			Guid existingEtag;
			bool deleted;
			var existingMetadata = TryGetExisting(id, out existingItem, out existingEtag, out deleted);
			if (existingMetadata == null)
			{
				log.Debug("New item {0} replicated successfully from {1}", id, Src);
				AddWithoutConflict(id, null, metadata, incoming);
				return;
			}


			// we just got the same version from the same source - request playback again?
			// at any rate, not an error, moving on
			if (existingMetadata.Value<string>(Constants.RavenReplicationSource) == metadata.Value<string>(Constants.RavenReplicationSource)
				&& existingMetadata.Value<long>(Constants.RavenReplicationVersion) == metadata.Value<long>(Constants.RavenReplicationVersion))
			{
				return;
			}


			var existingDocumentIsInConflict = existingMetadata[Constants.RavenReplicationConflict] != null;

			if (existingDocumentIsInConflict == false &&                    // if the current document is not in conflict, we can continue without having to keep conflict semantics
				(Historian.IsDirectChildOfCurrent(metadata, existingMetadata)))		// this update is direct child of the existing doc, so we are fine with overwriting this
			{
				log.Debug("Existing item {0} replicated successfully from {1}", id, Src);

				var etag = deleted == false ? existingEtag : (Guid?)null;
				AddWithoutConflict(id, etag, metadata, incoming);
				return;
			}

			if (TryResolveConflict(id, metadata, incoming, existingItem))
			{
				AddWithoutConflict(id, existingEtag, metadata, incoming);
				return;
			}

			Database.TransactionalStorage.ExecuteImmediatelyOrRegisterForSynchronization(() =>
			                                                                            Database.RaiseNotifications(new DocumentChangeNotification
			                                                                            {
			                                                                            	Id = id,
			                                                                            	Type = ReplicationConflict
			                                                                            }, metadata));

			var newDocumentConflictId = SaveConflictedItem(id, metadata, incoming, existingEtag);

			if (existingDocumentIsInConflict) // the existing document is in conflict
			{
				log.Debug("Conflicted item {0} has a new version from {1}, adding to conflicted documents", id, Src);

				AppendToCurrentItemConflicts(id, newDocumentConflictId, existingMetadata, existingItem);
				return;
			}
				log.Debug("Existing item {0} is in conflict with replicated version from {1}, marking item as conflicted", id, Src);

				// we have a new conflict
				// move the existing doc to a conflict and create a conflict document
				var existingDocumentConflictId = id + "/conflicts/" + HashReplicationIdentifier(existingEtag);

			CreateConflict(id, newDocumentConflictId, existingDocumentConflictId, existingItem, existingMetadata);
			}

		protected abstract DocumentChangeTypes ReplicationConflict { get; }

		private string SaveConflictedItem(string id, RavenJObject metadata, TExternal incoming, Guid existingEtag)
		{
			metadata[Constants.RavenReplicationConflictDocument] = true;
			var newDocumentConflictId = id + "/conflicts/" + HashReplicationIdentifier(metadata);
			metadata.Add(Constants.RavenReplicationConflict, RavenJToken.FromObject(true));
			AddWithoutConflict(
				newDocumentConflictId,
				null, // we explicitly want to overwrite a document if it already exists, since it  is known uniuque by the key 
				metadata, 
				incoming);
			return newDocumentConflictId;
		}

		private void ReplicateDelete(string id, RavenJObject metadata, TExternal incoming)
		{
			TInternal existingItem;
			Guid existingEtag;
			bool deleted;
			var existingMetadata = TryGetExisting(id, out existingItem, out existingEtag, out deleted);
			if (existingMetadata == null)
			{
				log.Debug("Replicating deleted item {0} from {1} that does not exist, ignoring", id, Src);
				return;
			}
			if (existingMetadata.Value<bool>(Constants.RavenReplicationConflict)) // locally conflicted
			{
				log.Debug("Replicating deleted item {0} from {1} that is already conflicted, adding to conflicts.", id, Src);
				var savedConflictedItemId = SaveConflictedItem(id, metadata, incoming, existingEtag);
				AppendToCurrentItemConflicts(id, savedConflictedItemId, existingMetadata, existingItem);
				return;
			}
			if(existingMetadata.Value<bool>(Constants.RavenDeleteMarker)) //deleted locally as well
			{
				log.Debug("Replicating deleted item {0} from {1} that was deleted locally. Merging histories", id, Src);
				var existingHistory = new RavenJArray(ReplicationData.GetHistory(existingMetadata));
				var newHistory = new RavenJArray(ReplicationData.GetHistory(metadata));

				foreach (var item in newHistory)
				{
					existingHistory.Add(item);
				}


				if (metadata.ContainsKey(Constants.RavenReplicationVersion) &&
					metadata.ContainsKey(Constants.RavenReplicationSource))
				{
					existingHistory.Add(new RavenJObject
						{
							{Constants.RavenReplicationVersion, metadata[Constants.RavenReplicationVersion]},
							{Constants.RavenReplicationSource, metadata[Constants.RavenReplicationSource]}
						});
				}

				while (existingHistory.Length > Constants.ChangeHistoryLength)
				{
					existingHistory.RemoveAt(0);
				}

				MarkAsDeleted(id, metadata);
				return;
			}
			if(Historian.IsDirectChildOfCurrent(metadata, existingMetadata))// not modified
			{
				log.Debug("Delete of existing item {0} was replicated successfully from {1}", id, Src);
				DeleteItem(id, existingEtag);
				MarkAsDeleted(id, metadata);
				return;
			}

			Database.TransactionalStorage.ExecuteImmediatelyOrRegisterForSynchronization(() =>
																						Database.RaiseNotifications(new DocumentChangeNotification
			{
																							Id = id,
																							Type = DocumentChangeTypes.ReplicationConflict
																						}, metadata));
				var newConflictId = SaveConflictedItem(id, metadata, incoming, existingEtag);
				log.Debug("Existing item {0} is in conflict with replicated delete from {1}, marking item as conflicted", id, Src);

				// we have a new conflict  move the existing doc to a conflict and create a conflict document
				var existingDocumentConflictId = id + "/conflicts/" + HashReplicationIdentifier(existingEtag);
			CreateConflict(id, newConflictId, existingDocumentConflictId, existingItem, existingMetadata);
			}

		protected abstract void DeleteItem(string id, Guid etag);

		protected abstract void MarkAsDeleted(string id, RavenJObject metadata);

		protected abstract void AddWithoutConflict(string id, Guid? etag, RavenJObject metadata, TExternal incoming);

		protected abstract void CreateConflict(string id, string newDocumentConflictId, string existingDocumentConflictId, TInternal existingItem, RavenJObject existingMetadata);

		protected abstract void AppendToCurrentItemConflicts(string id, string newConflictId, RavenJObject existingMetadata, TInternal existingItem);

		protected abstract RavenJObject TryGetExisting(string id, out TInternal existingItem, out Guid existingEtag, out bool deleted);

		protected abstract bool TryResolveConflict(string id, RavenJObject metadata, TExternal document,
												  TInternal existing);


		private static string HashReplicationIdentifier(RavenJObject metadata)
		{
			using (var md5 = MD5.Create())
			{
				var bytes = Encoding.UTF8.GetBytes(metadata.Value<string>(Constants.RavenReplicationSource) + "/" + metadata.Value<string>("@etag"));
				return new Guid(md5.ComputeHash(bytes)).ToString();
			}
		}

		private string HashReplicationIdentifier(Guid existingEtag)
		{
			using (var md5 = MD5.Create())
			{
				var bytes = Encoding.UTF8.GetBytes(Database.TransactionalStorage.Id + "/" + existingEtag);
				return new Guid(md5.ComputeHash(bytes)).ToString();
			}
		}
	}
}
