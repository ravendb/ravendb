using System;
using Newtonsoft.Json.Linq;
using Raven.Bundles.Versioning.Data;
using Raven.Database;
using Raven.Database.Plugins;
using System.Linq;
using Raven.Database.Json;

namespace Raven.Bundles.Versioning.Triggers
{
    public class VersioningPutTrigger : AbstractPutTrigger
    {
        public const string RavenDocumentRevision = "Raven-Document-Revision";
		public const string RavenDocumentParentRevision = "Raven-Document-Parent-Revision"; 
		public const string RavenDocumentRevisionStatus = "Raven-Document-Revision-Status";

        public override VetoResult AllowPut(string key, JObject document, JObject metadata, TransactionInformation transactionInformation)
        {
			if (VersioningContext.IsInVersioningContext)
				return VetoResult.Allowed;
			
			if (metadata.Value<string>(RavenDocumentRevisionStatus) == "Historical")
				return VetoResult.Deny("Modifying a historical revision is not allowed");
			return VetoResult.Allowed;
        }

        public override void OnPut(string key, JObject document, JObject metadata, TransactionInformation transactionInformation)
        {
			if (VersioningContext.IsInVersioningContext)
				return;
			if (key.StartsWith("Raven/", StringComparison.InvariantCultureIgnoreCase))
				return;

            if (metadata.Value<string>(RavenDocumentRevisionStatus) == "Historical")
                return;

            var versioningConfiguration = GetDocumentVersioningConfiguration(metadata);

            if (versioningConfiguration.Exclude)
				return;

			
			using(VersioningContext.Enter())
			{
				var copyMetadata = new JObject(metadata);
				copyMetadata[RavenDocumentRevisionStatus] = JToken.FromObject("Historical");
				copyMetadata.Remove(RavenDocumentRevision);
				var parentRevision = metadata.Value<string>(RavenDocumentRevision);
				if(parentRevision!=null)
				{
					copyMetadata[RavenDocumentParentRevision] = key + "/revisions/" + parentRevision;
					metadata[RavenDocumentParentRevision] = key + "/revisions/" + parentRevision;
				}

				PutResult newDoc = Database.Put(key + "/revisions/", null, document, copyMetadata,
											 transactionInformation);
				int revision = int.Parse(newDoc.Key.Split('/').Last());

                RemoveOldRevisions(key, revision, versioningConfiguration, transactionInformation);

				metadata[RavenDocumentRevisionStatus] = JToken.FromObject("Current");
				metadata[RavenDocumentRevision] = JToken.FromObject(revision);
			}
        }

        private VersioningConfiguration GetDocumentVersioningConfiguration(JObject metadata)
        {
            var versioningConfiguration = new VersioningConfiguration
            {
                MaxRevisions = Int32.MaxValue, 
                Exclude = false
            };
            var entityName = metadata.Value<string>("Raven-Entity-Name");
            if(entityName != null)
            {
                var doc = Database.Get("Raven/Versioning/" + entityName, null) ??
                          Database.Get("Raven/Versioning/DefaultConfiguration", null);
                if( doc != null)
                {
                    versioningConfiguration = doc.DataAsJson.JsonDeserialization<VersioningConfiguration>();
                }
            }
            return versioningConfiguration;
        }

        private void RemoveOldRevisions(string key, int revision, VersioningConfiguration versioningConfiguration, TransactionInformation transactionInformation)
        {
            int latestValidRevision = revision - versioningConfiguration.MaxRevisions;
            if (latestValidRevision <= 1)
                return;

        	Database.Delete(key + "/revisions/" + (latestValidRevision - 1), null, transactionInformation);
        }
    }
}
