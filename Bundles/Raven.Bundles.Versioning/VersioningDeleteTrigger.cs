using System;
using Raven.Database;
using Raven.Database.Plugins;

namespace Raven.Bundles.Versioning
{
    public class VersioningDeleteTrigger : AbstractDeleteTrigger
    {
        [ThreadStatic] internal static bool allowDeletiongOfHistoricalDocuments;

        public override VetoResult AllowDelete(string key, TransactionInformation transactionInformation)
        {
            if(allowDeletiongOfHistoricalDocuments)
                return VetoResult.Allowed;
            JsonDocument document = Database.Get(key, transactionInformation);
            if (document == null)
                return VetoResult.Allowed;
            if (document.Metadata.Value<string>(VersioningPutTrigger.RavenDocumentRevisionStatus) != "Historical")
                return VetoResult.Allowed;
            return VetoResult.Deny("Deleting a historical revision is not allowed");
        }
    }
}