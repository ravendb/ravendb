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
		public const string RavenDocumentParentRevision = "Raven-Document-Parent-Revision"; 
		public const string RavenDocumentRevisionStatus = "Raven-Document-Revision-Status";

        private int? maxRevisions;
        private string[] excludeByEntityName = new string[0];

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

			if (excludeByEntityName.Contains(metadata.Value<string>("Raven-Entity-Name")))
				return;

			if (metadata.Value<string>(RavenDocumentRevisionStatus) == "Historical")
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

				RemoveOldRevisions(key, revision, transactionInformation);

				metadata[RavenDocumentRevisionStatus] = JToken.FromObject("Current");
				metadata[RavenDocumentRevision] = JToken.FromObject(revision);
			}
        }

        private void RemoveOldRevisions(string key, int revision, TransactionInformation transactionInformation)
        {
            if (maxRevisions == null)
                return;

            int latestValidRevision = revision - maxRevisions.Value;
            if (latestValidRevision <= 1)
                return;

        	Database.Delete(key + "/revisions/" + (latestValidRevision - 1), null, transactionInformation);
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
