using System;
using System.Configuration;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Plugins;
using System.Linq;

namespace Raven.Bundles.Versioning
{
    public class VersioningPutTrigger : IPutTrigger, IRequiresDocumentDatabaseInitialization
    {
        private const string RavenDocumentRevision = "Raven-Document-Revision";
        private const string RavenDocumentRevisionStatus = "Raven-Document-Revision-Status";
        private DocumentDatabase docDb;
        private int? maxRevisions;

        public VetoResult AllowPut(string key, JObject document, JObject metadata, TransactionInformation transactionInformation)
        {
            if (metadata.Value<string>(RavenDocumentRevisionStatus) != "Historical")
                return VetoResult.Allowed;
            if(  docDb.Get(key, transactionInformation) == null)
                return VetoResult.Allowed;
          
            return VetoResult.Deny("Modifying a historical revision is not allowed");
        }

        public void OnPut(string key, JObject document, JObject metadata, TransactionInformation transactionInformation)
        {
            if (metadata.Value<string>(RavenDocumentRevisionStatus) == "Historical")
                return;

            int revision = 0;
            if (metadata[RavenDocumentRevision] != null)
                revision = metadata.Value<int>(RavenDocumentRevision);

            var copyMetadata = new JObject(metadata);
            copyMetadata[RavenDocumentRevisionStatus] = JToken.FromObject("Historical");
            copyMetadata[RavenDocumentRevision] = JToken.FromObject(revision +1);
            PutResult newDoc = docDb.Put(key + "/revisions/", null, document, copyMetadata,
                                         transactionInformation);
            revision = int.Parse(newDoc.Key.Split('/').Last());

            RemoveOldRevisions(key, revision, transactionInformation);

            metadata[RavenDocumentRevisionStatus] = JToken.FromObject("Current");
            metadata[RavenDocumentRevision] = JToken.FromObject(revision);
        }

        private void RemoveOldRevisions(string key, int revision, TransactionInformation transactionInformation)
        {
            if (maxRevisions == null)
                return;

            int latestValidRevision = revision - maxRevisions.Value;
            if (latestValidRevision <= 1)
                return;

            docDb.Delete(key + "/revisions/" + (latestValidRevision - 1), null, transactionInformation);
        }

        public void AfterCommit(string key, JObject document, JObject metadata)
        {
        }

        public void Initialize(DocumentDatabase database)
        {
            docDb = database;
            maxRevisions = database.Configuration.GetConfigurationValue<int>("Raven/Versioning/MaxRevisions");
        }
    }
}
