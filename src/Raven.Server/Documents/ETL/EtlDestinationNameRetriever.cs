using System;
using System.Linq;
using Raven.Client.Server.ETL;
using Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters;

namespace Raven.Server.Documents.ETL
{
    public static class EtlDestinationNameRetriever
    {
        public static string GetName(EtlDestination destination)
        {
            switch (destination)
            {
                case RavenDestination raven:
                    return $"{raven.Database}@{raven.Url}";
                case SqlDestination sql:
                    var dbAtServer = DbProviderFactories.GetDatabaseAndServerFromConnectionString(sql.Connection.FactoryName, sql.Connection.ConnectionString);
                    return $"{dbAtServer} [{string.Join(" ", sql.SqlTables.Select(x => x.TableName))}]";
                default:
                    throw new NotSupportedException($"Not supported type {destination.GetType()} of ETL destination");
            }
        }
    }
}