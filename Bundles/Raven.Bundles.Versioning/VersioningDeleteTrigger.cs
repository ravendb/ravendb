using System;
using Raven.Database;
using Raven.Database.Plugins;

namespace Raven.Bundles.Versioning
{
    public class VersioningDeleteTrigger : IDeleteTrigger, IRequiresDocumentDatabaseInitialization
    {
        private DocumentDatabase docDb;

        [ThreadStatic] internal static bool allowDeletiongOfHistoricalDocuments;

        public VetoResult AllowDelete(string key, TransactionInformation transactionInformation)
        {
            if(allowDeletiongOfHistoricalDocuments)
                return VetoResult.Allowed;
            JsonDocument document = docDb.Get(key, transactionInformation);
            if (document == null)
                return VetoResult.Allowed;
            if (document.Metadata.Value<string>(VersioningPutTrigger.RavenDocumentRevisionStatus) != "Historical")
                return VetoResult.Allowed;
            return VetoResult.Deny("Deleting a historical revision is not allowed");
        }

        public void OnDelete(string key, TransactionInformation transactionInformation)
        {
        }

        public void AfterCommit(string key)
        {
        }

        public void Initialize(DocumentDatabase database)
        {
            docDb = database;
        }
    }
}