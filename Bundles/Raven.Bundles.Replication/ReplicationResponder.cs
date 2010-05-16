using System;
using Newtonsoft.Json.Linq;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Responders;
using Raven.Database.Storage.StorageActions;

namespace Raven.Bundles.Replication
{
    public class ReplicationResponder : RequestResponder
    {
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
                Database.TransactionalStorage.Batch(actions =>
                {
                    string lastEtag = Guid.Empty.ToString();
                    foreach (JObject document in array)
                    {
                        var metadata = document.Value<JObject>("@metadata");
                        lastEtag = metadata.Value<string>("@etag");
                        var id = metadata.Value<string>("@id");
                        document.Remove("@metadata");
                        ReplicateDocument(actions, id, metadata, document);
                    }

                    Database.Put(ReplicationConstants.RavenReplicationSourcesBasePath + "/" + src, null,
                                 JObject.FromObject(new SourceReplicationInformation {LastEtag = new Guid(lastEtag)}),
                                 new JObject(), null);
                });
            }
        }

        private static void ReplicateDocument(DocumentStorageActions actions, string id, JObject metadata, JObject document)
        {

            var existingDoc = actions.DocumentByKey(id, null);
            var replicationSourceId = metadata.Value<string>(ReplicationConstants.RavenReplicationSource);
            if (existingDoc == null || replicationSourceId == null)
            {
                actions.AddDocument(id, null, document, metadata);
                return;
            }
            var existingDocumentReplicationSourceId = existingDoc.Metadata.Value<string>(ReplicationConstants.RavenReplicationSource);
            if(existingDocumentReplicationSourceId == null)
            {
                actions.AddDocument(id, null, document, metadata);
                return;
            }

            var existingDocumentIsInConflict = existingDoc.Metadata[ReplicationConstants.RavenReplicationConflict] != null;

            
            if (existingDocumentIsInConflict == false &&                    // if the current document is in conflict, we have to keep conflict semantics
                replicationSourceId == existingDocumentReplicationSourceId) // our last update from that server too, so we are fine with overwriting this
            {
                actions.AddDocument(id, null, document, metadata);
                return;
            }


            var newDocumentConflictId = id + "/conflicts/" + metadata.Value<string>("@etag");
            actions.AddDocument(newDocumentConflictId, null, document, metadata);

            if (existingDocumentIsInConflict) // the existing document is in conflict
            {
                // just update the current doc with the new conflict document
                existingDoc.DataAsJson.Value<JArray>("Conflicts").Add(JToken.FromObject(newDocumentConflictId));
                actions.AddDocument(id, existingDoc.Etag, existingDoc.DataAsJson, existingDoc.Metadata);
                return;
            }

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