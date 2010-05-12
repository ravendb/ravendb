using System;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Plugins;
using System.Linq;

namespace Raven.Bundles.Versioning
{
    public class VersioningPutTrigger : IPutTrigger, IRequiresDocumentDatabaseInitialization
    {
        public const string RavenDocumentRevision = "Raven-Document-Revision";
        public const string RavenDocumentRevisionStatus = "Raven-Document-Revision-Status";
        private DocumentDatabase docDb;
        private int? maxRevisions;
        private string[] excludeByEntityName = new string[0];

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

            if (excludeByEntityName.Contains(metadata.Value<string>("Raven-Entity-Name")))
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

            VersioningDeleteTrigger.allowDeletiongOfHistoricalDocuments = true;
            try
            {
                docDb.Delete(key + "/revisions/" + (latestValidRevision - 1), null, transactionInformation);
            }
            finally
            {
                VersioningDeleteTrigger.allowDeletiongOfHistoricalDocuments = false;    
            }
        }

        public void AfterCommit(string key, JObject document, JObject metadata)
        {
        }

        public void Initialize(DocumentDatabase database)
        {
            docDb = database;
            maxRevisions = database.Configuration.GetConfigurationValue<int>("Raven/Versioning/MaxRevisions");

            string value;
            if(database.Configuration.Settings.TryGetValue("Raven/Versioning/Exclude", out value)==false)
            {
                excludeByEntityName = new string[0];
                return;
            }
            excludeByEntityName = value
                .Split(new[] {';'}, StringSplitOptions.RemoveEmptyEntries)
                .Select(x => x.Trim())
                .ToArray();
        }
    }
}
