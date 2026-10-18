using System;
using System.Data.Common;
using Raven.Client.Documents.Operations.ETL.SQL;

namespace Raven.Client.Documents.Operations.CdcSink;

internal static class CdcSinkDefaultSchema
{
    /// <summary>
    /// Returns the schema name the CDC runtime substitutes when a <see cref="CdcSinkTableConfig.SourceTableSchema"/>
    /// is empty: <c>"public"</c> on Postgres, <c>"dbo"</c> on SQL Server, the connection's
    /// database name on MySQL. The CDC handlers must use this when resolving a saved
    /// configuration's tables, so the test endpoint matches what the runtime would do —
    /// Studio's schema-discovery output always carries explicit schema names, but a saved
    /// task may have left them empty to rely on the runtime default. Each per-provider
    /// <c>CdcSinkProcess</c> already passes the same value down to <c>CdcSinkDocumentProcessor</c>.
    /// </summary>
    internal static string Resolve(string factoryName, string connectionString) =>
        SqlProviderParser.GetSupportedProvider(factoryName) switch
        {
            SqlProvider.Npgsql => "public",
            SqlProvider.SqlClient => "dbo",
            SqlProvider.MySqlConnectorFactory => MySqlDatabaseOf(connectionString),
            _ => throw new NotSupportedException($"CDC sink does not support provider '{factoryName}'.")
        };

    private static string MySqlDatabaseOf(string connectionString)
    {
        var builder = new DbConnectionStringBuilder { ConnectionString = connectionString };

        return builder.TryGetValue("Database", out var database) || builder.TryGetValue("Initial Catalog", out database)
            ? (string)database
            : string.Empty;
    }
}
