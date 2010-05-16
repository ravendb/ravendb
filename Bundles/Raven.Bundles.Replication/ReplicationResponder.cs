using System;
using log4net;
using Newtonsoft.Json.Linq;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Responders;
using Raven.Database.Storage.StorageActions;

namespace Raven.Bundles.Replication
{
    public class ReplicationResponder : RequestResponder
    {
        private ILog log = LogManager.GetLogger(typeof (ReplicationResponder));

        public override void Respond(IHttpContext context)
        {
            var src = context.Request.QueryString["from"];
            if (string.IsNullOrEmpty(src))
            {
                context.SetStatusToBadRequest();
                return;
            }
            var array = context.ReadJsonArray();
            using (ReplicationContext.Enter())
            {
                log.DebugFormat("Got replication batch of {0} documents from {1}", array.Count, src);
                Database.TransactionalStorage.Batch(actions =>
                {
                    string lastEtag = Guid.Empty.ToString();
                    foreach (JObject document in array)
                    {
                        var metadata = document.Value<JObject>("@metadata");
                        if(metadata[ReplicationConstants.RavenReplicationSource] == null)
                        {
                            // not sure why, old document from when the user didn't have replciation
                            // that we suddenly decided to replicate, choose the source for that
                            metadata[ReplicationConstants.RavenReplicationSource] = JToken.FromObject(src);
                        }
                        lastEtag = metadata.Value<string>("@etag");
                        var id = metadata.Value<string>("@id");
                        document.Remove("@metadata");
                        ReplicateDocument(actions, id, metadata, document, src);
                    }

                    Database.Put(ReplicationConstants.RavenReplicationSourcesBasePath + "/" + src, null,
                                 JObject.FromObject(new SourceReplicationInformation {LastEtag = new Guid(lastEtag)}),
                                 new JObject(), null);
                });
            }
        }

        private void ReplicateDocument(DocumentStorageActions actions, string id, JObject metadata, JObject document, string src)
        {
            var existingDoc = actions.DocumentByKey(id, null);
            if (existingDoc == null)
            {
                log.DebugFormat("New document {0} replicated successfully from {1}", id, src);
                actions.AddDocument(id, Guid.Empty, document, metadata);
                return;
            }
            var replicationSourceId = metadata.Value<string>(ReplicationConstants.RavenReplicationSource);

            var existingDocumentReplicationSourceId = existingDoc.Metadata.Value<string>(ReplicationConstants.RavenReplicationSource);
            
            var existingDocumentIsInConflict = existingDoc.Metadata[ReplicationConstants.RavenReplicationConflict] != null;
            if (existingDocumentIsInConflict == false &&                    // if the current document is in conflict, we have to keep conflict semantics
                (replicationSourceId == existingDocumentReplicationSourceId)) // our last update from that server too, so we are fine with overwriting this
            {
                log.DebugFormat("Existing document {0} replicated successfully from {1}", id, src);
                actions.AddDocument(id, null, document, metadata);
                return;
            }


            var newDocumentConflictId = id + "/conflicts/" + metadata.Value<string>("@etag");
            actions.AddDocument(newDocumentConflictId, null, document, metadata);

            if (existingDocumentIsInConflict) // the existing document is in conflict
            {
                log.DebugFormat("Conflicted document {0} has a new version from {1}, adding to conflicted documents", id, src);
                
                // just update the current doc with the new conflict document
                existingDoc.DataAsJson.Value<JArray>("Conflicts").Add(JToken.FromObject(newDocumentConflictId));
                actions.AddDocument(id, existingDoc.Etag, existingDoc.DataAsJson, existingDoc.Metadata);
                return;
            }
            log.DebugFormat("Existing document {0} is in conflict with replicated version from {1}, marking document as conflicted", id, src);
                
            // we have a new conflict
            // move the existing doc to a conflict and create a conflict document
            var existingDocumentConflictId = id +"/conflicts/"+existingDoc.Etag;
            actions.AddDocument(existingDocumentConflictId, null, existingDoc.DataAsJson, existingDoc.Metadata);
            actions.AddDocument(id, null,
                                new JObject(
                                    new JProperty("Conflicts", new JArray(existingDocumentConflictId, newDocumentConflictId))),
                                new JObject(
                                    new JProperty(ReplicationConstants.RavenReplicationConflict, true), 
                                    new JProperty("@Http-Status-Code", 409),
                                    new JProperty("@Http-Status-Description", "Conflict")
                                    ));
        }

        public override string UrlPattern
        {
            get { return "^/replication/replicate$"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] { "POST" }; }
        }
    }
}