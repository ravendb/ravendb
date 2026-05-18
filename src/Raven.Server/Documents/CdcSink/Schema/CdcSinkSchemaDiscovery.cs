using System;
using System.Threading;
using System.Threading.Tasks;
using MySqlConnector;
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
        // ADO.NET factory names. Kept here so `For`, `IsSupportedFactoryName`, and
        // `ResolveDefaultSchema` all agree on the canonical strings and any future caller
        // (e.g. the CDC verify endpoint) can compare against the same identifiers instead
        // of repeating the literals.
        internal const string NpgsqlFactory = "Npgsql";
        internal const string SystemDataSqlClientFactory = "System.Data.SqlClient";
        internal const string MicrosoftDataSqlClientFactory = "Microsoft.Data.SqlClient";
        internal const string MySqlDataFactory = "MySql.Data.MySqlClient";
        internal const string MySqlConnectorFactory = "MySqlConnector.MySqlConnectorFactory";

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
                NpgsqlFactory => new PostgresCdcSinkSchemaDiscovery(),
                SystemDataSqlClientFactory or MicrosoftDataSqlClientFactory => new SqlServerCdcSinkSchemaDiscovery(),
                MySqlDataFactory or MySqlConnectorFactory => new MySqlCdcSinkSchemaDiscovery(),
                _ => throw new InvalidOperationException(UnsupportedProviderMessage(factoryName)),
            };
        }

        /// <summary>
        /// True if the CDC sink subsystem (verify / schema / test / runtime) supports the given
        /// ADO.NET factory name. <see cref="DatabaseDriverDispatcher.GetProviderFromFactoryName"/>
        /// accepts a wider set (including Oracle) because the SQL Migration feature supports it,
        /// but CDC does not — so the CDC admin endpoints must gate on this narrower list.
        /// </summary>
        public static bool IsSupportedFactoryName(string factoryName)
        {
            return factoryName is NpgsqlFactory
                or SystemDataSqlClientFactory or MicrosoftDataSqlClientFactory
                or MySqlDataFactory or MySqlConnectorFactory;
        }

        /// <summary>
        /// Returns the schema name the CDC runtime substitutes when a <see cref="CdcSinkTableConfig.SourceTableSchema"/>
        /// is empty: <c>"public"</c> on Postgres, <c>"dbo"</c> on SQL Server, the connection's
        /// database name on MySQL. The CDC handlers must use this when resolving a saved
        /// configuration's tables, so the test endpoint matches what the runtime would do —
        /// Studio's schema-discovery output always carries explicit schema names, but a saved
        /// task may have left them empty to rely on the runtime default. Each per-provider
        /// <c>CdcSinkProcess</c> already passes the same value down to <c>CdcSinkDocumentProcessor</c>.
        /// </summary>
        public static string ResolveDefaultSchema(string factoryName, string connectionString)
        {
            return factoryName switch
            {
                NpgsqlFactory => "public",
                SystemDataSqlClientFactory or MicrosoftDataSqlClientFactory => "dbo",
                MySqlDataFactory or MySqlConnectorFactory
                    => new MySqlConnectionStringBuilder(connectionString).Database ?? "mysql",
                _ => throw new InvalidOperationException(UnsupportedProviderMessage(factoryName)),
            };
        }

        internal static string UnsupportedProviderMessage(string factoryName) =>
            $"CDC sink does not support provider '{factoryName}'. " +
            $"Supported providers: {NpgsqlFactory} (PostgreSQL), " +
            $"{SystemDataSqlClientFactory} / {MicrosoftDataSqlClientFactory} (SQL Server), " +
            $"{MySqlDataFactory} / {MySqlConnectorFactory} (MySQL/MariaDB).";
    }
}
