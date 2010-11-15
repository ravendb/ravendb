using System;
using log4net;
using Newtonsoft.Json.Linq;
using Raven.Bundles.Replication.Data;
using Raven.Database;
using Raven.Database.Data;
using Raven.Database.Server.Responders;
using Raven.Database.Storage;
using Raven.Http.Abstractions;
using Raven.Http.Extensions;
using Raven.Database.Json;

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
            using (ReplicationContext.Enter())
            {
                Database.TransactionalStorage.Batch(actions =>
                {
                    byte[] lastEtag = Guid.Empty.ToByteArray();
                    foreach (JObject attachment in array)
                    {
                        var metadata = attachment.Value<JObject>("@metadata");
                        if(metadata[ReplicationConstants.RavenReplicationSource] == null)
                        {
                            // not sure why, old attachment from when the user didn't have replciation
                            // that we suddenly decided to replicate, choose the source for that
                            metadata[ReplicationConstants.RavenReplicationSource] = JToken.FromObject(src);
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
                                 JObject.FromObject(new SourceReplicationInformation
                                 {
                                     LastDocumentEtag = lastDocId,
                                     LastAttachmentEtag = new Guid(lastEtag)
                                 }),
                                 new JObject(), null);
                });
            }
        }

        private void ReplicateAttachment(IStorageActionsAccessor actions, string id, JObject metadata, byte[] data, Guid lastEtag ,string src)
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


            var newDocumentConflictId = id + "/conflicts/" + lastEtag;
            metadata.Add(ReplicationConstants.RavenReplicationConflict, JToken.FromObject(true));
            actions.Attachments.AddAttachment(newDocumentConflictId, null, data, metadata);

            if (existingDocumentIsInConflict) // the existing document is in conflict
            {
                log.DebugFormat("Conflicted document {0} has a new version from {1}, adding to conflicted documents", id, src);
                
                // just update the current doc with the new conflict document
                existingAttachment.Metadata.Value<JArray>("Conflicts").Add(JToken.FromObject(newDocumentConflictId));
                actions.Attachments.AddAttachment(id, existingAttachment.Etag, existingAttachment.Data, existingAttachment.Metadata);
                return;
            }
            log.DebugFormat("Existing document {0} is in conflict with replicated version from {1}, marking document as conflicted", id, src);
                
            // we have a new conflict
            // move the existing doc to a conflict and create a conflict document
            var existingDocumentConflictId = id +"/conflicts/"+existingAttachment.Etag;
            
            existingAttachment.Metadata.Add(ReplicationConstants.RavenReplicationConflict, JToken.FromObject(true));
            actions.Attachments.AddAttachment(existingDocumentConflictId, null, existingAttachment.Data, existingAttachment.Metadata);
            actions.Attachments.AddAttachment(id, null,
                                new JObject(
                                    new JProperty("Conflicts", new JArray(existingDocumentConflictId, newDocumentConflictId))).ToBytes(),
                                new JObject(
                                    new JProperty(ReplicationConstants.RavenReplicationConflict, true), 
                                    new JProperty("@Http-Status-Code", 409),
                                    new JProperty("@Http-Status-Description", "Conflict")
                                    ));
        }

        private static bool IsDirectChildOfCurrentAttachment(Attachment existingDoc, JObject metadata)
        {
            return JToken.DeepEquals(existingDoc.Metadata[ReplicationConstants.RavenReplicationVersion],
                                     metadata[ReplicationConstants.RavenReplicationParentVersion]) && 
                   JToken.DeepEquals(existingDoc.Metadata[ReplicationConstants.RavenReplicationSource],
                                     metadata[ReplicationConstants.RavenReplicationParentSource]);
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
