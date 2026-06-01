using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Client.Documents.Operations.ETL.SQL;
using DbProviderFactories = Raven.Server.Documents.ETL.Providers.RelationalDatabase.SQL.RelationalWriters.DbProviderFactories;

namespace Raven.Server.Documents.CdcSink;

/// <summary>
/// Verifies that a source SQL database is properly configured for CDC and folds the result into a
/// schema-discovery response: connection-level findings (provisioning permission, server-wide
/// configuration) land on the <see cref="CdcSinkSourceSchema"/> top level, per-table findings on the
/// individual <see cref="CdcSinkSourceTable"/>. Whether CDC is already active per table is set by
/// <see cref="Schema.CdcSinkSchemaDiscovery"/>, so this stage does not re-derive it.
/// </summary>
public static class CdcSinkSourceVerifier
{
    /// <summary>
    /// Connects to the source and annotates <paramref name="schema"/> in place with verification
    /// findings. Connection/provider failures are surfaced as <see cref="CdcSinkSourceSchema.Errors"/>
    /// rather than thrown, so the caller can return the partially-populated schema.
    /// </summary>
    internal static async Task AnnotateAsync(SqlConnectionString connection, CdcSinkSourceSchema schema, CancellationToken token = default)
    {
        DbProviderFactory factory;
        try
        {
            factory = DbProviderFactories.GetFactory(connection.FactoryName);
        }
        catch (Exception e)
        {
            schema.Errors.Add($"Could not find database provider factory '{connection.FactoryName}': {e}");
            return;
        }

        DbConnection dbConnection;
        try
        {
            dbConnection = factory.CreateConnection();
            if (dbConnection == null)
            {
                schema.Errors.Add($"Provider factory '{connection.FactoryName}' returned a null connection.");
                return;
            }
        }
        catch (Exception e)
        {
            schema.Errors.Add($"Could not create connection using factory '{connection.FactoryName}': {e}");
            return;
        }

        await using (dbConnection)
        {
            dbConnection.ConnectionString = connection.ConnectionString;

            try
            {
                await dbConnection.OpenAsync(token);
            }
            catch (Exception e)
            {
                schema.Errors.Add($"Could not connect to source database: {e}");
                return;
            }

            try
            {
                switch (connection.FactoryName)
                {
                    case "Npgsql":
                        await VerifyPostgreSqlAsync(dbConnection, schema, token);
                        break;

                    case "System.Data.SqlClient":
                    case "Microsoft.Data.SqlClient":
                        await VerifySqlServerAsync(dbConnection, schema, token);
                        break;

                    case "MySql.Data.MySqlClient":
                    case "MySqlConnector.MySqlConnectorFactory":
                        await VerifyMySqlAsync(dbConnection, schema, token);
                        break;

                    default:
                        schema.Errors.Add(
                            $"CDC is not supported for provider '{connection.FactoryName}'. " +
                            "Supported providers: Npgsql (PostgreSQL), System.Data.SqlClient / Microsoft.Data.SqlClient (SQL Server), MySql.Data.MySqlClient / MySqlConnector (MySQL/MariaDB).");
                        break;
                }
            }
            catch (Exception e)
            {
                schema.Errors.Add($"Error during source database verification: {e}");
            }
        }
    }

    private static async Task VerifyPostgreSqlAsync(DbConnection connection, CdcSinkSourceSchema schema, CancellationToken token)
    {
        using var qb = new Npgsql.NpgsqlCommandBuilder();

        // Check wal_level = logical
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "SHOW wal_level";
            var walLevel = (await cmd.ExecuteScalarAsync(token))?.ToString();

            if (string.Equals(walLevel, "logical", StringComparison.OrdinalIgnoreCase) == false)
            {
                schema.Errors.Add(
                    $"PostgreSQL wal_level is '{walLevel}', but must be 'logical' for CDC. " +
                    "Set wal_level = logical in postgresql.conf and restart the server.");
            }
        }

        // Check the connecting user's privilege to provision replication infrastructure.
        string currentUser;
        bool hasReplicationPrivilege;
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "SELECT current_user, rolreplication FROM pg_roles WHERE rolname = current_user";
            await using var reader = await cmd.ExecuteReaderAsync(token);
            if (await reader.ReadAsync(token) == false)
            {
                schema.Errors.Add("Could not determine the current database user's privileges.");
                return;
            }

            currentUser = reader.GetString(0);
            hasReplicationPrivilege = reader.GetBoolean(1);
        }

        if (hasReplicationPrivilege)
        {
            schema.HasPermissionToSetup = true;
        }
        else
        {
            schema.Errors.Add(
                $"User '{currentUser}' does not have the REPLICATION privilege required to provision CDC. " +
                $"Either grant it with: ALTER ROLE {qb.QuoteIdentifier(currentUser)} REPLICATION; " +
                "or have an administrator create the replication slot and publication before creating the CDC task.");
        }

        await AnnotatePostgresReplicaIdentityAsync(connection, schema, token);
    }

    /// <summary>
    /// Per-table check: a table whose REPLICA IDENTITY can't carry row-identifying columns on DELETE
    /// gets a table-scoped warning. NOTHING sends no columns; DEFAULT sends the primary key only, which
    /// is useless when the table has no primary key. FULL and INDEX are left to the configuration-time
    /// join-column check (<see cref="CheckReplicaIdentityCoversColumns"/>) run by the CDC process.
    /// </summary>
    private static async Task AnnotatePostgresReplicaIdentityAsync(DbConnection connection, CdcSinkSourceSchema schema, CancellationToken token)
    {
        using var qb = new Npgsql.NpgsqlCommandBuilder();

        foreach (var table in schema.Tables)
        {
            var schemaName = string.IsNullOrEmpty(table.SourceTableSchema) ? "public" : table.SourceTableSchema;

            char replicaIdentity;
            await using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = """
                    SELECT relreplident
                    FROM pg_class c
                    JOIN pg_namespace n ON c.relnamespace = n.oid
                    WHERE n.nspname = @schema AND c.relname = @table
                    """;
                CdcSinkProcess.AddParameter(cmd, "@schema", schemaName);
                CdcSinkProcess.AddParameter(cmd, "@table", table.SourceTableName);
                var result = await cmd.ExecuteScalarAsync(token);
                replicaIdentity = ReadReplicaIdentity(result);
            }

            string problem = replicaIdentity switch
            {
                'n' => "REPLICA IDENTITY is set to NOTHING, so DELETE events carry no columns.",
                'd' when table.PrimaryKeyColumns.Count == 0 =>
                    "the table has no primary key and REPLICA IDENTITY is DEFAULT, so DELETE events carry no row-identifying columns.",
                _ => null
            };

            if (problem == null)
                continue;

            table.Warnings.Add(
                $"Table '{schemaName}.{table.SourceTableName}': {problem} " +
                $"Set REPLICA IDENTITY FULL to capture the full row on delete: " +
                $"ALTER TABLE {qb.QuoteIdentifier(schemaName)}.{qb.QuoteIdentifier(table.SourceTableName)} REPLICA IDENTITY FULL;");
        }
    }

    /// <summary>
    /// Reads the single <c>pg_class.relreplident</c> scalar. Postgres' internal <c>"char"</c> type
    /// maps to <see cref="char"/> under the pinned Npgsql, but tolerate a <see cref="string"/> in
    /// case the driver / type mapping ever changes; unknown shapes fall back to 'd' (DEFAULT).
    /// </summary>
    private static char ReadReplicaIdentity(object scalar) => scalar switch
    {
        char c => c,
        string s when s.Length > 0 => s[0],
        _ => 'd'
    };

    /// <summary>
    /// Checks whether a table's REPLICA IDENTITY covers a set of required columns (PK + join columns).
    /// Returns null if the identity is sufficient, or an error message describing the problem.
    ///
    /// Rules:
    ///   'd' (DEFAULT, PK-only) → insufficient, the required columns aren't all in the PK
    ///                            (caller already verified this before calling us)
    ///   'n' (NOTHING) → insufficient, no columns sent on DELETE
    ///   'f' (FULL) → always sufficient, all columns sent on DELETE
    ///   'i' (INDEX) → sufficient only if the index covers all required columns
    /// </summary>
    internal static async Task<string> CheckReplicaIdentityCoversColumns(
        DbConnection connection, string schema, string table, HashSet<string> requiredColumns)
    {
        char replicaIdentity;
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = """
                SELECT relreplident
                FROM pg_class c
                JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE n.nspname = @schema AND c.relname = @table
                """;
            CdcSinkProcess.AddParameter(cmd, "@schema", schema);
            CdcSinkProcess.AddParameter(cmd, "@table", table);
            var result = await cmd.ExecuteScalarAsync();
            replicaIdentity = ReadReplicaIdentity(result);
        }

        switch (replicaIdentity)
        {
            case 'f':
                return null; // FULL — all columns sent, always sufficient

            case 'i':
            {
                // INDEX — check that the index covers all required columns.
                // pg_class.relreplident = 'i' means a specific index is designated.
                // Query the index columns via pg_index + pg_attribute.
                var indexColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                await using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = """
                        SELECT a.attname
                        FROM pg_index idx
                        JOIN pg_class c ON c.oid = idx.indrelid
                        JOIN pg_namespace n ON n.oid = c.relnamespace
                        JOIN pg_attribute a ON a.attrelid = idx.indexrelid
                        WHERE n.nspname = @schema AND c.relname = @table
                          AND idx.indisreplident = true
                        """;
                    CdcSinkProcess.AddParameter(cmd, "@schema", schema);
                    CdcSinkProcess.AddParameter(cmd, "@table", table);
                    await using var reader = await cmd.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                        indexColumns.Add(reader.GetString(0));
                }

                var missing = new List<string>();
                foreach (var col in requiredColumns)
                {
                    if (indexColumns.Contains(col) == false)
                        missing.Add(col);
                }

                if (missing.Count > 0)
                {
                    return $"REPLICA IDENTITY is set to INDEX, but the designated index does not cover " +
                           $"column(s): {string.Join(", ", missing)}.";
                }

                return null; // INDEX covers all required columns
            }

            case 'n':
                return "REPLICA IDENTITY is set to NOTHING — no columns are sent on DELETE.";

            default: // 'd' (DEFAULT) or unknown
                return "REPLICA IDENTITY is DEFAULT (PK-only) and the required columns are not all in the primary key.";
        }

    }

    private static async Task VerifySqlServerAsync(DbConnection connection, CdcSinkSourceSchema schema, CancellationToken token)
    {
        // Get database name for error messages
        string dbName;
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "SELECT DB_NAME()";
            dbName = (await cmd.ExecuteScalarAsync(token))?.ToString() ?? "unknown";
        }

        // Check SQL Server Agent status — required for CDC capture/cleanup jobs
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = @"
                SELECT dss.[status]
                FROM sys.dm_server_services dss
                WHERE dss.[servicename] LIKE N'SQL Server Agent (%'";

            var agentStatus = await cmd.ExecuteScalarAsync(token);

            // agentStatus = 4 means running. Null means we can't check (insufficient permissions, which is fine).
            if (agentStatus != null && agentStatus != DBNull.Value && Convert.ToInt32(agentStatus) != 4)
            {
                schema.Warnings.Add(
                    "SQL Server Agent is not running. CDC capture jobs require SQL Server Agent to be active. " +
                    "Changes will not be captured until the Agent is started.");
            }
        }

        // Check if user has db_owner permissions
        bool hasPermission;
        await using (var permCmd = connection.CreateCommand())
        {
            permCmd.CommandText = "SELECT IS_MEMBER('db_owner')";
            var memberResult = await permCmd.ExecuteScalarAsync(token);
            hasPermission = memberResult != null && memberResult != DBNull.Value && Convert.ToInt32(memberResult) == 1;
        }

        if (hasPermission)
            schema.HasPermissionToSetup = true;

        // Check if CDC is enabled on the database
        bool isCdcEnabled;
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()";
            var cdcResult = await cmd.ExecuteScalarAsync(token);
            isCdcEnabled = cdcResult != null && cdcResult != DBNull.Value && Convert.ToInt32(cdcResult) == 1;
        }

        if (isCdcEnabled == false)
        {
            if (hasPermission)
            {
                schema.Warnings.Add(
                    $"CDC is not enabled on database '{dbName}'. It will be enabled automatically when the task starts.");
            }
            else
            {
                schema.Errors.Add(
                    $"CDC is not enabled on database '{dbName}' and the current user does not have db_owner permissions to enable it. " +
                    "Ask a database administrator to run: EXEC sys.sp_cdc_enable_db;");
            }
            return;
        }

        // Check CDC capture/cleanup job health
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = @"
                SELECT job_type, job_error
                FROM msdb.dbo.cdc_jobs
                WHERE database_id = DB_ID()";

            try
            {
                await using var reader = await cmd.ExecuteReaderAsync(token);
                while (await reader.ReadAsync(token))
                {
                    var jobType = reader.GetString(reader.GetOrdinal("job_type"));
                    var jobError = reader.IsDBNull(reader.GetOrdinal("job_error"))
                        ? 0
                        : reader.GetInt32(reader.GetOrdinal("job_error"));

                    if (jobError != 0)
                    {
                        schema.Warnings.Add(
                            $"CDC {jobType} job in database '{dbName}' has error code {jobError}. " +
                            "This may indicate the CDC infrastructure is unhealthy.");
                    }
                }
            }
            catch (DbException)
            {
                // User may not have access to msdb — not a fatal error for verification
            }
        }
    }

    private static async Task VerifyMySqlAsync(DbConnection connection, CdcSinkSourceSchema schema, CancellationToken token)
    {
        // Detect MySQL vs MariaDB
        string version;
        bool isMariaDb;
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "SELECT VERSION()";
            version = (await cmd.ExecuteScalarAsync(token))?.ToString() ?? "";
            isMariaDb = version.Contains("MariaDB", StringComparison.OrdinalIgnoreCase);
        }

        schema.HasPermissionToSetup = true; // MySQL CDC uses binlog replication, no special setup commands needed

        // Check binlog_format
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "SELECT @@binlog_format";
            var format = (await cmd.ExecuteScalarAsync(token))?.ToString();
            if (string.Equals(format, "ROW", StringComparison.OrdinalIgnoreCase) == false)
            {
                schema.Errors.Add(
                    $"MySQL binlog_format is '{format}' but must be 'ROW' for CDC Sink. " +
                    (isMariaDb ? "MariaDB defaults to MIXED — " : "") +
                    "Set it with: SET GLOBAL binlog_format = 'ROW'; or add binlog_format = ROW to my.cnf.");
            }
        }

        // Check binlog_row_image
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "SELECT @@binlog_row_image";
            var rowImage = (await cmd.ExecuteScalarAsync(token))?.ToString();
            if (string.Equals(rowImage, "FULL", StringComparison.OrdinalIgnoreCase) == false)
            {
                schema.Errors.Add(
                    $"MySQL binlog_row_image is '{rowImage}' but must be 'FULL' for CDC Sink. " +
                    "Set it with: SET GLOBAL binlog_row_image = 'FULL';");
            }
        }

        // Verify GTID is enabled (MySQL only — MariaDB always has GTID support)
        if (isMariaDb == false)
        {
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT @@gtid_mode";
            var gtidMode = (await cmd.ExecuteScalarAsync(token))?.ToString();
            if (string.Equals(gtidMode, "ON", StringComparison.OrdinalIgnoreCase) == false)
            {
                schema.Errors.Add(
                    $"MySQL gtid_mode is '{gtidMode}' but must be 'ON' for CDC Sink. " +
                    "Enable it with: SET GLOBAL gtid_mode = ON; SET GLOBAL enforce_gtid_consistency = ON;");
            }
        }
    }
}
