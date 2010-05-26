using System;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Plugins;
using System.Linq;

namespace Raven.Bundles.Versioning
{
    public class VersioningPutTrigger : AbstractPutTrigger
    {
        public const string RavenDocumentRevision = "Raven-Document-Revision";
        public const string RavenDocumentRevisionStatus = "Raven-Document-Revision-Status";
        private int? maxRevisions;
        private string[] excludeByEntityName = new string[0];

        public override VetoResult AllowPut(string key, JObject document, JObject metadata, TransactionInformation transactionInformation)
        {
            if (metadata.Value<string>(RavenDocumentRevisionStatus) != "Historical")
                return VetoResult.Allowed;
            if (Database.Get(key, transactionInformation) == null)
                return VetoResult.Allowed;
          
            return VetoResult.Deny("Modifying a historical revision is not allowed");
        }

        public override void OnPut(string key, JObject document, JObject metadata, TransactionInformation transactionInformation)
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
            PutResult newDoc = Database.Put(key + "/revisions/", null, document, copyMetadata,
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
                Database.Delete(key + "/revisions/" + (latestValidRevision - 1), null, transactionInformation);
            }
            finally
            {
                VersioningDeleteTrigger.allowDeletiongOfHistoricalDocuments = false;    
            }
        }
        public override void Initialize()
        {
            maxRevisions = Database.Configuration.GetConfigurationValue<int>("Raven/Versioning/MaxRevisions");

            string value;
            if(Database.Configuration.Settings.TryGetValue("Raven/Versioning/Exclude", out value)==false)
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
