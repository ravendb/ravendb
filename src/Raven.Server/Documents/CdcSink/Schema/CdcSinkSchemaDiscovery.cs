using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.CdcSink.Schema;

namespace Raven.Server.Documents.CdcSink.Schema
{
    /// <summary>
    /// Source-side schema browser for the CDC Sink mapping UI. Each provider implementation
    /// enumerates tables / columns / PKs / FKs from the source database and annotates the
    /// result with CDC-specific hints (suggested column type, capturability, table enrollment).
    /// Consumed by <c>POST /admin/cdc-sink/schema</c>.
    /// </summary>
    public abstract class CdcSinkSchemaDiscovery
    {
        public abstract Task<CdcSinkSourceSchema> DiscoverAsync(string connectionString, string[] schemas, CancellationToken ct);

        /// <summary>
        /// Factory keyed on <see cref="Raven.Client.Documents.Operations.ETL.SQL.SqlConnectionString.FactoryName"/>.
        /// The accepted strings mirror <see cref="CdcSinkSourceVerifier.VerifyAsync"/> so the
        /// two endpoints stay aligned on which providers CDC supports.
        /// </summary>
        public static CdcSinkSchemaDiscovery For(string factoryName)
        {
            return factoryName switch
            {
                "Npgsql" => new PostgresCdcSinkSchemaDiscovery(),
                "System.Data.SqlClient" or "Microsoft.Data.SqlClient" => new SqlServerCdcSinkSchemaDiscovery(),
                "MySql.Data.MySqlClient" or "MySqlConnector.MySqlConnectorFactory" => new MySqlCdcSinkSchemaDiscovery(),
                _ => throw new InvalidOperationException(
                    $"CDC schema discovery is not supported for provider '{factoryName}'. " +
                    "Supported providers: Npgsql (PostgreSQL), System.Data.SqlClient / Microsoft.Data.SqlClient (SQL Server), " +
                    "MySql.Data.MySqlClient / MySqlConnector (MySQL/MariaDB)."),
            };
        }
    }
}
