//-----------------------------------------------------------------------
// <copyright file="AttachmentReplicationResponder.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using log4net;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Bundles.Replication.Data;
using Raven.Database.Data;
using Raven.Database.Server.Responders;
using Raven.Database.Storage;
using Raven.Http.Abstractions;
using Raven.Http.Extensions;
using Raven.Database.Json;
using Raven.Json.Linq;
using System.Linq;

namespace Raven.Bundles.Replication.Reponsders
{
    public class AttachmentReplicationResponder : RequestResponder
    {
        private ILog log = LogManager.GetLogger(typeof (DocumentReplicationResponder));

        public override void Respond(IHttpContext context)
        {
            var src = context.Request.QueryString["from"];
            if (string.IsNullOrEmpty(src))
            {
                context.SetStatusToBadRequest();
                return;
            }
            while (src.EndsWith("/"))
                src = src.Substring(0, src.Length - 1);// remove last /, because that has special meaning for Raven
            if (string.IsNullOrEmpty(src))
            {
                context.SetStatusToBadRequest();
                return;
            }
			var array = context.ReadBsonArray();
            using (Database.DisableAllTriggersForCurrentThread())
            {
                Database.TransactionalStorage.Batch(actions =>
                {
                    byte[] lastEtag = Guid.Empty.ToByteArray();
                    foreach (RavenJObject attachment in array)
                    {
                        var metadata = attachment.Value<RavenJObject>("@metadata");
                        if(metadata[ReplicationConstants.RavenReplicationSource] == null)
                        {
                            // not sure why, old attachment from when the user didn't have replciation
                            // that we suddenly decided to replicate, choose the source for that
                            metadata[ReplicationConstants.RavenReplicationSource] = RavenJToken.FromObject(src);
                        }
                        lastEtag = attachment.Value<byte[]>("@etag");
                        var id = attachment.Value<string>("@id");
                        ReplicateAttachment(actions, id, metadata, attachment.Value<byte[]>("data"), new Guid(lastEtag), src);
                    }


                    var replicationDocKey = ReplicationConstants.RavenReplicationSourcesBasePath + "/" + src;
                    var replicationDocument = Database.Get(replicationDocKey,null);
                    var lastDocId = Guid.Empty;
                    if(replicationDocument != null)
                    {
                        lastDocId =
                            replicationDocument.DataAsJson.JsonDeserialization<SourceReplicationInformation>().
                                LastDocumentEtag;
                    }
                    Database.Put(replicationDocKey, null,
                                 RavenJObject.FromObject(new SourceReplicationInformation
                                 {
                                     LastDocumentEtag = lastDocId,
                                     LastAttachmentEtag = new Guid(lastEtag)
                                 }),
                                 new RavenJObject(), null);
                });
            }
        }

        private void ReplicateAttachment(IStorageActionsAccessor actions, string id, RavenJObject metadata, byte[] data, Guid lastEtag ,string src)
        {
            var existingAttachment = actions.Attachments.GetAttachment(id);
            if (existingAttachment == null)
            {
                log.DebugFormat("New attachment {0} replicated successfully from {1}", id, src);
				actions.Attachments.AddAttachment(id, Guid.Empty, data, metadata);
                return;
            }
            
            var existingDocumentIsInConflict = existingAttachment.Metadata[ReplicationConstants.RavenReplicationConflict] != null;
            if (existingDocumentIsInConflict == false &&                    // if the current document is not in conflict, we can continue without having to keep conflict semantics
                (IsDirectChildOfCurrentAttachment(existingAttachment, metadata))) // this update is direct child of the existing doc, so we are fine with overwriting this
            {
                log.DebugFormat("Existing document {0} replicated successfully from {1}", id, src);
                actions.Attachments.AddAttachment(id, null, data, metadata);
                return;
            }


            var newDocumentConflictId = id + "/conflicts/" +
				metadata.Value<string>(ReplicationConstants.RavenReplicationSource) + 
				"/" + lastEtag;
            metadata.Add(ReplicationConstants.RavenReplicationConflict, RavenJToken.FromObject(true));
            actions.Attachments.AddAttachment(newDocumentConflictId, null, data, metadata);

            if (existingDocumentIsInConflict) // the existing document is in conflict
            {
                log.DebugFormat("Conflicted document {0} has a new version from {1}, adding to conflicted documents", id, src);
                
                // just update the current doc with the new conflict document
                existingAttachment.Metadata.Value<RavenJArray>("Conflicts").Add(RavenJToken.FromObject(newDocumentConflictId));
                actions.Attachments.AddAttachment(id, existingAttachment.Etag, existingAttachment.Data, existingAttachment.Metadata);
                return;
            }
            log.DebugFormat("Existing document {0} is in conflict with replicated version from {1}, marking document as conflicted", id, src);
                
            // we have a new conflict
            // move the existing doc to a conflict and create a conflict document
			var existingDocumentConflictId = id + "/conflicts/" + Database.TransactionalStorage.Id + "/" + existingAttachment.Etag;
            
            existingAttachment.Metadata.Add(ReplicationConstants.RavenReplicationConflict, RavenJToken.FromObject(true));
            actions.Attachments.AddAttachment(existingDocumentConflictId, null, existingAttachment.Data, existingAttachment.Metadata);
            actions.Attachments.AddAttachment(id, null,
                                new RavenJObject
                                {
                                	{"Conflicts", new RavenJArray(existingDocumentConflictId, newDocumentConflictId)
									}
								}.ToBytes(),
                                new RavenJObject
                                {
                                	{ReplicationConstants.RavenReplicationConflict, true},
									{"@Http-Status-Code", 409},
									{"@Http-Status-Description", "Conflict"}
								});
        }

        private static bool IsDirectChildOfCurrentAttachment(Attachment existingAttachment, RavenJObject metadata)
        {
        	var version = new RavenJObject
        	{
        		{ReplicationConstants.RavenReplicationSource, existingAttachment.Metadata[ReplicationConstants.RavenReplicationSource]},
        		{ReplicationConstants.RavenReplicationVersion, existingAttachment.Metadata[ReplicationConstants.RavenReplicationVersion]},
        	};

			var history = metadata[ReplicationConstants.RavenReplicationHistory];
			if (history == null) // no history, not a parent
				return false;

        	return history.Values().Contains(version, new RavenJTokenEqualityComparer());
        }

    	public override string UrlPattern
        {
            get { return "^/replication/replicateAttachments$"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] { "POST" }; }
        }
    }
}
