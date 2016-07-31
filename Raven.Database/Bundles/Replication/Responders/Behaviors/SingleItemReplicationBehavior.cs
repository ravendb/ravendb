using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Json.Linq;
using Raven.Abstractions.Logging;
using Raven.Database.Bundles.Replication.Impl;
using Raven.Database.Bundles.Replication.Triggers;
using Raven.Database.Storage;
using Raven.Json.Linq;

namespace Raven.Database.Bundles.Replication.Responders.Behaviors
{
    public abstract class SingleItemReplicationBehavior<TInternal, TExternal>
    {
        protected class CreatedConflict
        {
            public Etag Etag { get; set; }
            public string[] ConflictedIds { get; set; }
        }

        private readonly ILog log = LogManager.GetCurrentClassLogger();
        public DocumentDatabase Database { get; set; }
        public IStorageActionsAccessor Actions { get; set; }
        public string Src { get; set; }

        public void Replicate(string id, RavenJObject metadata, TExternal incoming)
        {
            if (metadata.Value<bool>(Constants.RavenDeleteMarker))
            {
                ReplicateDelete(id, metadata, incoming);
                return;
            }
            TInternal existingItem;
            Etag existingEtag;
            bool deleted;

            RavenJObject existingMetadata;

            try
            {
                existingMetadata = TryGetExisting(id, out existingItem, out existingEtag, out deleted);
            }
            catch (Exception e)
            {
                log.ErrorException(string.Format("Replication - fetching existing item failed. (key = {0})", id), e);
                throw new InvalidOperationException("Replication - fetching existing item failed. (key = " + id + ")", e);
            }

            if (existingMetadata == null)
            {
                AddWithoutConflict(id, null, metadata, incoming);
                if (log.IsDebugEnabled)
                    log.Debug("New item {0} replicated successfully from {1}", id, Src);
                return;
            }

            // we just got the same version from the same source - request playback again?
            // at any rate, not an error, moving on
            if (existingMetadata.Value<string>(Constants.RavenReplicationSource) ==
                metadata.Value<string>(Constants.RavenReplicationSource)
                &&
                existingMetadata.Value<long>(Constants.RavenReplicationVersion) ==
                metadata.Value<long>(Constants.RavenReplicationVersion))
            {
                return;
            }

            var existingDocumentIsInConflict = existingMetadata[Constants.RavenReplicationConflict] != null;

            if (existingDocumentIsInConflict == false &&
                // if the current document is not in conflict, we can continue without having to keep conflict semantics
                (Historian.IsDirectChildOfCurrent(metadata, existingMetadata)))
                // this update is direct child of the existing doc, so we are fine with overwriting this
            {
                var etag = deleted == false ? existingEtag : null;
                AddWithoutConflict(id, etag, metadata, incoming);
                if (log.IsDebugEnabled)
                    log.Debug("Existing item {0} replicated successfully from {1}", id, Src);
                return;
            }

            RavenJObject resolvedMetadataToSave;
            TExternal resolvedItemToSave;
            if (TryResolveConflict(id, metadata, incoming, existingItem, out resolvedMetadataToSave, out resolvedItemToSave))
            {                
                if (metadata.ContainsKey("Raven-Remove-Document-Marker") &&
                   metadata.Value<bool>("Raven-Remove-Document-Marker"))
                {
                    if (resolvedMetadataToSave.ContainsKey(Constants.RavenEntityName))
                        metadata[Constants.RavenEntityName] = resolvedMetadataToSave[Constants.RavenEntityName];
                    DeleteItem(id, null);
                    MarkAsDeleted(id, metadata);
                }
                else
                {
                    var etag = deleted == false ? existingEtag : null;
                    var resolvedItemJObject = resolvedItemToSave as RavenJObject;
                    if (resolvedItemJObject != null)
                        ExecuteRemoveConflictOnPutTrigger(id, metadata, resolvedItemJObject);

                    AddWithoutConflict(id, etag, resolvedMetadataToSave, resolvedItemToSave);

                }
                return;
            }
            //this is expensive but worth trying if we can avoid conflicts
            if (TryResolveConflictByCheckingIfIdentical(metadata, incoming, existingItem, out resolvedMetadataToSave))
            {
                //The metadata here is merged (changed), it needs to be pushed.
                AddWithoutConflict(id,null, resolvedMetadataToSave,incoming);
                return;
            }

            CreatedConflict createdConflict;

            var newDocumentConflictId = SaveConflictedItem(id, metadata, incoming, existingEtag);

            if (existingDocumentIsInConflict) // the existing document is in conflict
            {
                if (log.IsDebugEnabled)
                    log.Debug("Conflicted item {0} has a new version from {1}, adding to conflicted documents", id, Src);

                createdConflict = AppendToCurrentItemConflicts(id, newDocumentConflictId, existingMetadata, existingItem);
            }
            else
            {
                if (log.IsDebugEnabled)
                    log.Debug("Existing item {0} is in conflict with replicated version from {1}, marking item as conflicted", id, Src);
                // we have a new conflict
                // move the existing doc to a conflict and create a conflict document
                var existingDocumentConflictId = id + "/conflicts/" + GetReplicationIdentifierForCurrentDatabase();

                createdConflict = CreateConflict(id, newDocumentConflictId, existingDocumentConflictId, existingItem,
                                                  existingMetadata);
            }

            Database.TransactionalStorage.ExecuteImmediatelyOrRegisterForSynchronization(() =>
                Database.Notifications.RaiseNotifications(new ReplicationConflictNotification()
                                                                    {
                                                                        Id = id,
                                                                        Etag = createdConflict.Etag,
                                                                        ItemType = ReplicationConflict,
                                                                        OperationType = ReplicationOperationTypes.Put,
                                                                        Conflicts = createdConflict.ConflictedIds
                                                                    }));
            }

        public void ResolveConflict(string id, RavenJObject metadata, TExternal incoming, TInternal existingItem)
        {
            RavenJObject resolvedMetadataToSave;
            TExternal resolvedItemToSave;
            if (TryResolveConflict(id, metadata, incoming, existingItem, out resolvedMetadataToSave, out resolvedItemToSave))
            {
                if (metadata.ContainsKey("Raven-Remove-Document-Marker") &&
                    metadata.Value<bool>("Raven-Remove-Document-Marker"))
                {
                    if(resolvedMetadataToSave.ContainsKey(Constants.RavenEntityName))
                        metadata[Constants.RavenEntityName] = resolvedMetadataToSave[Constants.RavenEntityName];
                    DeleteItem(id, null);
                    MarkAsDeleted(id, metadata);
                }
                else
                {
                    var resolvedItemJObject = resolvedItemToSave as RavenJObject;
                    if (resolvedItemJObject != null)
                        ExecuteRemoveConflictOnPutTrigger(id, metadata, resolvedItemJObject);
                    resolvedMetadataToSave.Remove(Constants.RavenReplicationConflict);
                    resolvedMetadataToSave.Remove(Constants.RavenReplicationConflictDocument);
                    AddWithoutConflict(id, null, resolvedMetadataToSave, resolvedItemToSave);

                }
            }
        }

        private void ExecuteRemoveConflictOnPutTrigger(string id, RavenJObject metadata, RavenJObject resolvedItemJObject)
        {
//since we are in replication handler, triggers are disabled, and if we are replicating PUT of conflict resolution,
            //we need to execute the relevant trigger manually
            // --> AddWithoutConflict() does PUT, but because of 'No Triggers' context the trigger itself is executed
            var removeConflictTrigger = Database.PutTriggers.GetAllParts()
                .Select(trg => trg.Value)
                .OfType<RemoveConflictOnPutTrigger>()
                .FirstOrDefault();

            Debug.Assert(removeConflictTrigger != null, "If this is null, this means something is very wrong - replication configured, and no relevant plugin is there.");
            removeConflictTrigger.OnPut(id, resolvedItemJObject, new RavenJObject(metadata), null);
        }

        protected abstract ReplicationConflictTypes ReplicationConflict { get; }

        private string SaveConflictedItem(string id, RavenJObject metadata, TExternal incoming, Etag existingEtag)
        {
            metadata[Constants.RavenReplicationConflictDocument] = true;
            var newDocumentConflictId = id + "/conflicts/" + GetReplicationIdentifier(metadata);
            metadata.Add(Constants.RavenReplicationConflict, RavenJToken.FromObject(true));
            AddWithoutConflict(
                newDocumentConflictId,
                null, // we explicitly want to overwrite a document if it already exists, since it  is known uniuque by the key 
                metadata, 
                incoming);
            return newDocumentConflictId;
        }

        private void ReplicateDelete(string id, RavenJObject newMetadata, TExternal incoming)
        {
            TInternal existingItem;
            Etag existingEtag;
            bool deleted;
            var existingMetadata = TryGetExisting(id, out existingItem, out existingEtag, out deleted);
            if (existingMetadata == null)
            {
                if (log.IsDebugEnabled)
                    log.Debug("Replicating deleted item {0} from {1} that does not exist, ignoring.", id, Src);
                return;
            }
            if (existingMetadata.ContainsKey(Constants.RavenEntityName))
                newMetadata[Constants.RavenEntityName] = existingMetadata[Constants.RavenEntityName];
            RavenJObject currentReplicationEntry = null;
            if (newMetadata.ContainsKey(Constants.RavenReplicationVersion) &&
                newMetadata.ContainsKey(Constants.RavenReplicationSource))
            {
                currentReplicationEntry = new RavenJObject
                {
                    {Constants.RavenReplicationVersion, newMetadata[Constants.RavenReplicationVersion]},
                    {Constants.RavenReplicationSource, newMetadata[Constants.RavenReplicationSource]}
                };
            }
                        
            var existingHistory = ReplicationData.GetHistory(existingMetadata);
            if (currentReplicationEntry != null &&
                existingHistory.Any(x => RavenJTokenEqualityComparer.Default.Equals(
                    ((RavenJObject)x)[Constants.RavenReplicationSource], currentReplicationEntry[Constants.RavenReplicationSource])
                    && ((RavenJObject)x)[Constants.RavenReplicationVersion].Value<long>() >= currentReplicationEntry[Constants.RavenReplicationVersion].Value<long>()))
            {
                if (log.IsDebugEnabled)
                    log.Debug("Replicated delete for {0} already exist in item history, ignoring", id);
                return;
            }
            
            if (existingMetadata.Value<bool>(Constants.RavenDeleteMarker)) //deleted locally as well
            {
                if (log.IsDebugEnabled)
                    log.Debug("Replicating deleted item {0} from {1} that was deleted locally. Merging histories.", id, Src);

                var newHistory = ReplicationData.GetHistory(newMetadata);
                if (currentReplicationEntry != null)
                    newHistory.Add(currentReplicationEntry);

                //Merge histories
                ReplicationData.SetHistory(newMetadata, Historian.MergeReplicationHistories(newHistory, existingHistory));
                newMetadata[Constants.RavenReplicationMergedHistory] = true;
                MarkAsDeleted(id, newMetadata);

                return;
            }

            if (Historian.IsDirectChildOfCurrent(newMetadata, existingMetadata)) // not modified
            {
                if (log.IsDebugEnabled)
                    log.Debug("Delete of existing item {0} was replicated successfully from {1}", id, Src);
                DeleteItem(id, existingEtag);
                MarkAsDeleted(id, newMetadata);
                return;
            }

            CreatedConflict createdConflict;

            if (existingMetadata.Value<bool>(Constants.RavenReplicationConflict)) // locally conflicted
            {
                if (log.IsDebugEnabled)
                    log.Debug("Replicating deleted item {0} from {1} that is already conflicted, adding to conflicts.", id, Src);
                var savedConflictedItemId = SaveConflictedItem(id, newMetadata, incoming, existingEtag);
                createdConflict = AppendToCurrentItemConflicts(id, savedConflictedItemId, existingMetadata, existingItem);
            }
            else
            {
                RavenJObject resolvedMetadataToSave;
                TExternal resolvedItemToSave;
                if (TryResolveConflict(id, newMetadata, incoming, existingItem, out resolvedMetadataToSave, out resolvedItemToSave))
                {
                    AddWithoutConflict(id, existingEtag, resolvedMetadataToSave, resolvedItemToSave);
                    return;
                }
                var newConflictId = SaveConflictedItem(id, newMetadata, incoming, existingEtag);
                if (log.IsDebugEnabled)
                    log.Debug("Existing item {0} is in conflict with replicated delete from {1}, marking item as conflicted", id, Src);

                // we have a new conflict  move the existing doc to a conflict and create a conflict document
                var existingDocumentConflictId = id + "/conflicts/" + GetReplicationIdentifierForCurrentDatabase();
                createdConflict = CreateConflict(id, newConflictId, existingDocumentConflictId, existingItem, existingMetadata);
            }

            Database.TransactionalStorage.ExecuteImmediatelyOrRegisterForSynchronization(() =>
                Database.Notifications.RaiseNotifications(new ReplicationConflictNotification()
                                                        {
                                                            Id = id,
                                                            Etag = createdConflict.Etag,
                                                            Conflicts = createdConflict.ConflictedIds,
                                                            ItemType = ReplicationConflictTypes.DocumentReplicationConflict,
                                                            OperationType = ReplicationOperationTypes.Delete
                                                        }));

            }

        protected abstract void DeleteItem(string id, Etag etag);

        protected abstract void MarkAsDeleted(string id, RavenJObject metadata);

        protected abstract void AddWithoutConflict(string id, Etag etag, RavenJObject metadata, TExternal incoming);

        protected abstract CreatedConflict CreateConflict(string id, string newDocumentConflictId,
            string existingDocumentConflictId, TInternal existingItem, RavenJObject existingMetadata);

        protected abstract CreatedConflict AppendToCurrentItemConflicts(string id, string newConflictId,
            RavenJObject existingMetadata, TInternal existingItem);

        protected abstract RavenJObject TryGetExisting(string id, out TInternal existingItem, out Etag existingEtag,
            out bool deleted);

        protected abstract bool TryResolveConflict(string id, RavenJObject metadata, TExternal document,
            TInternal existing, out RavenJObject resolvedMetadataToSave,
                                        out TExternal resolvedItemToSave);

        /// <summary>
        /// Tries to resolve the conflict by checking if the items are identical.
        /// This is a seperate method since it is expensive and we don't want to 
        /// run this method unless we faield all the other conflict resolvers.
        /// </summary>
        /// <param name="metadata">The metadata of the incoming object</param>
        /// <param name="document">The incoming object data</param>
        /// <param name="existing">The existing object</param>
        /// <param name="resolvedMetadataToSave">The metadata to save</param>
        /// <returns></returns>
        protected abstract bool TryResolveConflictByCheckingIfIdentical(RavenJObject metadata, TExternal document,
            TInternal existing, out RavenJObject resolvedMetadataToSave);

        /// <summary>
        /// Runs shallow equal on the metadata while ignoring keys starting with '@'
        /// And replication related properties like replication
        /// </summary>
        /// <param name="origin"></param>
        /// <param name="external"></param>
        /// <param name="result">The output metadata incase the metadata are equal</param>
        /// <returns></returns>
        protected static bool CheckIfMetadataIsEqualEnoughForReplicationAndMergeHistorires(RavenJObject origin, RavenJObject external, out RavenJObject result)
        {
            result = null;
            var keysToCheck = new HashSet<string>(external.Keys.Where(k => !k.StartsWith("@") && !IgnoreProperties.Contains(k)));
            foreach (var key in origin.Keys.Where(k=>!k.StartsWith("@") && !IgnoreProperties.Contains(k)))
            {
                var originVal = origin[key];                
                RavenJToken externalVal;
                if (external.TryGetValue(key, out externalVal) == false)
                    return false;
                if (!RavenJTokenEqualityComparer.Default.Equals(originVal, externalVal))
                    return false;
                keysToCheck.Remove(key);
            }
            if(keysToCheck.Any())
                return false;
            //If we got here the metadata is the same, need to merge histories
            MergeReplicationHistories(origin, external, ref result);
            return true;
        }

        private static void MergeReplicationHistories(RavenJObject origin, RavenJObject external, ref RavenJObject result)
        {
            result = (RavenJObject) origin.CloneToken();
            RavenJToken originHistory;
            RavenJToken externalHisotry;
            var originHasHistory = origin.TryGetValue(Constants.RavenReplicationHistory, out originHistory);
            var externalHasHistory = external.TryGetValue(Constants.RavenReplicationHistory, out externalHisotry);
            RavenJToken externalVersion;
            RavenJToken externalSource;
            //we are going to lose the external source and version if we don't add them here
            if (external.TryGetValue(Constants.RavenReplicationVersion, out externalVersion)
                && external.TryGetValue(Constants.RavenReplicationSource, out externalSource))
            {
                if (externalHasHistory)
                {
                    externalHisotry = externalHisotry.CloneToken();
                }
                else
                {
                    externalHisotry = new RavenJArray();
                }
                var historyEntry = new RavenJObject();
                historyEntry[Constants.RavenReplicationVersion] = externalVersion;
                historyEntry[Constants.RavenReplicationSource] = externalSource;
                ((RavenJArray)externalHisotry).Add(historyEntry);
                externalHasHistory = true;
            }
            RavenJArray mergedHistory = null;
            //need to merge histories
            if (originHasHistory)
            {
                mergedHistory = Historian.MergeReplicationHistories((RavenJArray) originHistory, (RavenJArray) externalHisotry);
                result[Constants.RavenReplicationMergedHistory] = true;
            }
            else if (externalHasHistory)
            {
                //this might be a snapshot if somehow there was an history but no version or source
                mergedHistory = (RavenJArray)(externalHisotry.IsSnapshot? externalHisotry.CloneToken(): externalHisotry);
            }

            //if the original has history and the external didn't we already cloned it.
            if (mergedHistory != null)
                result[Constants.RavenReplicationHistory] = mergedHistory;
        }

        private static readonly HashSet<string> IgnoreProperties = new HashSet<string>
        {
            Constants.RavenReplicationSource,
            Constants.RavenReplicationVersion,
            Constants.RavenReplicationHistory,
            Constants.RavenReplicationMergedHistory,
            Constants.RavenLastModified,
            Constants.LastModified,
            "Content-Type"
        };
        private static string GetReplicationIdentifier(RavenJObject metadata)
        {
            return metadata.Value<string>(Constants.RavenReplicationSource);
            }

        private string GetReplicationIdentifierForCurrentDatabase()
        {
            return Database.TransactionalStorage.Id.ToString();
            }
        }
    }
