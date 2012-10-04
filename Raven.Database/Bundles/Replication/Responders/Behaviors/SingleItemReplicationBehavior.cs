using System;
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
			var existingMetadata = TryGetExisting(id, out existingItem, out existingEtag);
			if (existingMetadata == null)
			{
				log.Debug("New item {0} replicated successfully from {1}", id, Src);
				AddWithoutConflict(id, null, metadata, incoming);
				return;
			}


			// we just got the same version from the same source - request playback again?
			// at any rate, not an error, moving on
			if (existingMetadata.Value<string>(Constants.RavenReplicationSource) == metadata.Value<string>(Constants.RavenReplicationSource)
			    && existingMetadata.Value<int>(Constants.RavenReplicationVersion) == metadata.Value<int>(Constants.RavenReplicationVersion))
			{
				return;
			}


			var existingDocumentIsInConflict = existingMetadata[Constants.RavenReplicationConflict] != null;
			if (existingDocumentIsInConflict == false &&                    // if the current document is not in conflict, we can continue without having to keep conflict semantics
			    (IsDirectChildOfCurrent(metadata, existingMetadata))) // this update is direct child of the existing doc, so we are fine with overwriting this
			{
				log.Debug("Existing item {0} replicated successfully from {1}", id, Src);
				AddWithoutConflict(id, existingEtag, metadata, incoming);
				return;
			}

			if (TryResolveConflict(id, metadata, incoming, existingItem))
			{
				AddWithoutConflict(id, existingEtag, metadata, incoming);
				return;
			}

			Database.TransactionalStorage.ExecuteImmediatelyOrRegisterForSyncronization(() =>
			                                                                            Database.RaiseNotifications(new DocumentChangeNotification
			                                                                            {
			                                                                            	Name = id,
			                                                                            	Type = ReplicationConflict
			                                                                            }));

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
			AddWithoutConflict(newDocumentConflictId, Guid.Empty, metadata, incoming);
			return newDocumentConflictId;
		}

		private void ReplicateDelete(string id, RavenJObject metadata, TExternal incoming)
		{
			TInternal existingItem;
			Guid existingEtag;
			var existingMetadata = TryGetExisting(id, out existingItem, out existingEtag);
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
				var existingHistory = existingMetadata.Value<RavenJArray>(Constants.RavenReplicationHistory) ?? new RavenJArray();
				var newHistory = metadata.Value<RavenJArray>(Constants.RavenReplicationHistory) ?? new RavenJArray();

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
			if(IsDirectChildOfCurrent(metadata, existingMetadata))// not modified
			{
				log.Debug("Delete of existing item {0} was replicated successfully from {1}", id, Src);
				DeleteItem(id, existingEtag);
				MarkAsDeleted(id, metadata);
				return;
			}

			Database.TransactionalStorage.ExecuteImmediatelyOrRegisterForSyncronization(() =>
																						Database.RaiseNotifications(new DocumentChangeNotification
																						{
																							Name = id,
																							Type = DocumentChangeTypes.ReplicationConflict
																						}));
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

		protected abstract RavenJObject TryGetExisting(string id, out TInternal existingItem, out Guid existingEtag);

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


		private static bool IsDirectChildOfCurrent(RavenJObject incomingMetadata, RavenJObject existingMetadata)
		{
			var version = new RavenJObject
			{
				{Constants.RavenReplicationSource, existingMetadata[Constants.RavenReplicationSource]},
				{Constants.RavenReplicationVersion, existingMetadata[Constants.RavenReplicationVersion]},
			};

			var history = incomingMetadata[Constants.RavenReplicationHistory];
			if (history == null || history.Type == JTokenType.Null) // no history, not a parent
				return false;

			if (history.Type != JTokenType.Array)
				return false;

			return history.Values().Contains(version, new RavenJTokenEqualityComparer());
		}
	}
}