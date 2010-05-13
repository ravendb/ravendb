using System;
using System.Linq;
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
            var array = context.ReadJsonArray();
            Database.TransactionalStorage.Batch(actions =>
            {
                foreach (JObject document in array)
                {
                    var metadata = document.Value<JObject>("@metadata");
                    var id = metadata.Value<string>("@id");
                    ReplicateDocument(actions, id, metadata, document);
                }
            });
        }

        private static void ReplicateDocument(DocumentStorageActions actions, string id, JObject metadata, JObject document)
        {

            var existingDoc = actions.DocumentByKey(id, null);
            if(existingDoc == null)
            {
                actions.AddDocument(id, null, document, metadata);
                return;
            }
            var ancestry = metadata.Value<JArray>(ReplicationConstants.RavenAncestry).Cast<JValue>().Select(x=>new Guid(x.Value<string>()));
            if(ancestry.Contains(existingDoc.Etag)) // fast-forward, essentially
            {
                actions.AddDocument(id, null, document, metadata);
                return;
            }
            var newDocumentConflictId = id + "/conflicts/" + metadata.Value<string>("ETag");
            actions.AddDocument(newDocumentConflictId, null, document, metadata);

            if (existingDoc.Metadata[ReplicationConstants.RavenAncestry] != null) // the existing document is in conflict
            {
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