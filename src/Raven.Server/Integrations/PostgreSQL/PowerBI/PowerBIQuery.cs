using Raven.Client.Documents;

namespace Raven.Server.Integrations.PostgreSQL.PowerBI
{
    public static class PowerBIQuery
    {
        public static bool TryParse(string queryText, int[] parametersDataTypes, IDocumentStore documentStore, out PgQuery pgQuery)
        {
            if (PBIFetchQuery.TryParse(queryText, parametersDataTypes, documentStore, out pgQuery))
            {
                return true;
            }

            if (PBIAllCollectionsQuery.TryParse(queryText, parametersDataTypes, documentStore, out pgQuery))
            {
                return true;
            }

            if (PBIPreviewQuery.TryParse(queryText, documentStore, out pgQuery))
            {
                return true;
            }

            pgQuery = null;
            return false;
        }
    }
}
