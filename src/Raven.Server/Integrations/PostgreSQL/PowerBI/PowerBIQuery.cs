using Raven.Server.Documents;

namespace Raven.Server.Integrations.PostgreSQL.PowerBI
{
    public static class PowerBIQuery
    {
        public static bool TryParse(string queryText, int[] parametersDataTypes, DocumentDatabase documentDatabase, out PgQuery pgQuery)
        {
            if (PBIFetchQuery.TryParse(queryText, parametersDataTypes, documentDatabase, out pgQuery))
            {
                return true;
            }

            if (PBIAllCollectionsQuery.TryParse(queryText, parametersDataTypes, documentDatabase, out pgQuery))
            {
                return true;
            }

            if (PBIPreviewQuery.TryParse(queryText, documentDatabase, out pgQuery))
            {
                return true;
            }

            pgQuery = null;
            return false;
        }
    }
}
