using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Mono.CSharp;
using NetTopologySuite.Operation.Buffer;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Bundles.Replication.Plugins;
using Raven.Database.Bundles.Replication.Responders.Behaviors;
using Raven.Database.Impl;
using Raven.Json.Linq;
using Sparrow;

namespace Raven.Bundles.Replication.Responders
{
    [Obsolete("Use RavenFS instead.")]
    public class AttachmentReplicationBehavior : SingleItemReplicationBehavior<Attachment, byte[]>
    {
        public IEnumerable<AbstractAttachmentReplicationConflictResolver> ReplicationConflictResolvers { get; set; }

        protected override ReplicationConflictTypes ReplicationConflict
        {
            get { return ReplicationConflictTypes.AttachmentReplicationConflict; }
        }

        protected override void DeleteItem(string id, Etag etag)
        {
            Database.Attachments.DeleteStatic(id, etag);
        }

        protected override void MarkAsDeleted(string id, RavenJObject metadata)
        {
            Actions.Lists.Set(Constants.RavenReplicationAttachmentsTombstones, id, metadata, UuidType.Attachments);
        }

        protected override void AddWithoutConflict(string id, Etag etag, RavenJObject metadata, byte[] incoming)
        {
            Database.Attachments.PutStatic(id, etag, new MemoryStream(incoming), metadata);
            Actions.Lists.Remove(Constants.RavenReplicationAttachmentsTombstones, id);
        }

        protected override CreatedConflict CreateConflict(string id, string newDocumentConflictId, string existingDocumentConflictId, Attachment existingItem, RavenJObject existingMetadata)
        {
            existingItem.Metadata.Add(Constants.RavenReplicationConflict, RavenJToken.FromObject(true));
            Actions.Attachments.AddAttachment(existingDocumentConflictId, null, existingItem.Data(), existingItem.Metadata);
            Actions.Lists.Remove(Constants.RavenReplicationDocsTombstones, id);
            var conflictsArray = new RavenJArray(existingDocumentConflictId, newDocumentConflictId);
            var conflictAttachment = new RavenJObject
            {
                {"Conflicts", conflictsArray}
            };
            var memoryStream = new MemoryStream();
            conflictAttachment.WriteTo(memoryStream);
            memoryStream.Position = 0;
            var etag = existingMetadata.Value<bool>(Constants.RavenDeleteMarker) ? null : existingItem.Etag;
            var newEtag = Actions.Attachments.AddAttachment(id, etag,
                                              memoryStream,
                                              new RavenJObject
                                              {
                                                  {Constants.RavenReplicationConflict, true},
                                                  {"@Http-Status-Code", 409},
                                                  {"@Http-Status-Description", "Conflict"}
                                              });
            return new CreatedConflict()
            {
                Etag = newEtag,
                ConflictedIds = conflictsArray.Select(x => x.Value<string>()).ToArray()
            };
        }

        protected override CreatedConflict AppendToCurrentItemConflicts(string id, string newConflictId, RavenJObject existingMetadata, Attachment existingItem)
        {
            var existingConflict = existingItem.Data().ToJObject();

            // just update the current attachment with the new conflict document
            RavenJArray conflictArray;
            existingConflict["Conflicts"] = conflictArray = new RavenJArray(existingConflict.Value<RavenJArray>("Conflicts"));

            var conflictEtag = existingItem.Etag;
            if (conflictArray.Contains(newConflictId) == false)
            {
                conflictArray.Add(newConflictId);

                var memoryStream = new MemoryStream();
                existingConflict.WriteTo(memoryStream);
                memoryStream.Position = 0;

                var newETag = Actions.Attachments.AddAttachment(id, existingItem.Etag, memoryStream, existingItem.Metadata);
                conflictEtag = newETag;
            }
                
            return new CreatedConflict
            {
                Etag = conflictEtag,
                ConflictedIds = conflictArray.Select(x => x.Value<string>()).ToArray()
            };
        }

        protected override RavenJObject TryGetExisting(string id, out Attachment existingItem, out Etag existingEtag, out bool deleted)
        {
            var existingAttachment = Actions.Attachments.GetAttachment(id);
            if (existingAttachment != null)
            {
                existingItem = existingAttachment;
                existingEtag = existingAttachment.Etag;
                deleted = false;
                return existingAttachment.Metadata;
            }

            var listItem = Actions.Lists.Read(Constants.RavenReplicationAttachmentsTombstones, id);
            if (listItem != null)
            {
                existingEtag = listItem.Etag;
                existingItem = new Attachment
                {
                    Etag = listItem.Etag,
                    Key = listItem.Key,
                    Metadata = listItem.Data,
                    Data = () => new MemoryStream()
                };
                deleted = true;
                return listItem.Data;
            }
            deleted = false;
            existingEtag = Etag.Empty;
            existingItem = null;
            return null;

        }

        protected override bool TryResolveConflict(string id, RavenJObject metadata, byte[] data, Attachment existing, out RavenJObject metadataToSave,
                                        out byte[] dataToSave)
        {
            foreach (var replicationConflictResolver in ReplicationConflictResolvers)
            {
                if (replicationConflictResolver.TryResolveConflict(id, metadata, data, existing, Actions.Attachments.GetAttachment,
                                                           out metadataToSave, out dataToSave))
                    return true;
            }

            metadataToSave = null;
            dataToSave = null;

            return false;
        }

        protected override bool TryResolveConflictByCheckingIfIdentical(RavenJObject metadata, byte[] document, Attachment existing, out RavenJObject resolvedMetadataToSave)
        {
            //if the metadata is not equal there is no reason the compare the data
            if (CheckIfMetadataIsEqualEnoughForReplicationAndMergeHistorires(existing.Metadata, metadata, out resolvedMetadataToSave) == false)
                return false;
            var data = existing.Data();
            //It would have been better to check the length of the stream before reading it,
            // but the stream type is sometimes MemoryStream and sometimes BufferedStream
            // and we will end up reading the whole stream just to get its legnth anyway.
            var dataAsArray = data.ReadData();
            if (dataAsArray.Length != document.Length)
                return false;
            unsafe
            {
                fixed (byte* right = dataAsArray)
                fixed( byte* left = document)
                {
                    return Memory.Compare(right, left, document.Length) == 0;
                }
            }
        }
    }
}
