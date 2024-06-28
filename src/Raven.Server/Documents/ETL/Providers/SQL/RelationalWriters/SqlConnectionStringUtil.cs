namespace Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters;

public static class SqlConnectionStringUtil
{
    public static string GetConnectionStringWithOptionalEncrypt(string connectionString)
    {
        connectionString = connectionString.Trim();
        
        if (connectionString.EndsWith(';') == false)
            connectionString += ";";

        connectionString += "Encrypt=Optional";

        return connectionString;
    }
}
