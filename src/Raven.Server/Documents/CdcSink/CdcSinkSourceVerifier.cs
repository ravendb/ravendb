using System;
using System.Collections.Generic;

using System.Data.Common;
using Raven.Server.Documents.ETL.Providers.RelationalDatabase.SQL.RelationalWriters;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.ETL.SQL;
using DbProviderFactories = Raven.Server.Documents.ETL.Providers.RelationalDatabase.SQL.RelationalWriters.DbProviderFactories;

namespace Raven.Server.Documents.CdcSink;

/// <summary>
/// Verifies that a source SQL database is properly configured for CDC.
/// Checks both whether the user has permissions to set up CDC, and whether
/// CDC is already configured (for reduced-privilege scenarios where an admin
/// set it up separately).
/// </summary>
public static class CdcSinkSourceVerifier
{
    public static async Task<CdcSinkVerificationResult> VerifyAsync(SqlConnectionString connection, List<string> tableNames = null, CdcSinkConfiguration configuration = null)
    {
        var result = new CdcSinkVerificationResult();

        DbProviderFactory factory;
        try
        {
            factory = DbProviderFactories.GetFactory(connection.FactoryName);
        }
        catch (Exception e)
        {
            result.Errors.Add($"Could not find database provider factory '{connection.FactoryName}': {e}");
            return result;
        }

        DbConnection dbConnection;
        try
        {
            dbConnection = factory.CreateConnection();
            if (dbConnection == null)
            {
                result.Errors.Add($"Provider factory '{connection.FactoryName}' returned a null connection.");
                return result;
            }
        }
        catch (Exception e)
        {
            result.Errors.Add($"Could not create connection using factory '{connection.FactoryName}': {e}");
            return result;
        }

        await using (dbConnection)
        {
            dbConnection.ConnectionString = connection.ConnectionString;

            try
            {
                await dbConnection.OpenAsync();
            }
            catch (Exception e)
            {
                result.Errors.Add($"Could not connect to source database: {e}");
                return result;
            }

            try
            {
                switch (connection.FactoryName)
                {
                    case "Npgsql":
                        await VerifyPostgreSqlAsync(dbConnection, tableNames, configuration, result);
                        break;

                    case "System.Data.SqlClient":
                    case "Microsoft.Data.SqlClient":
                        await VerifySqlServerAsync(dbConnection, tableNames, result);
                        break;

                    default:
                        result.Errors.Add(
                            $"CDC is not supported for provider '{connection.FactoryName}'. " +
                            "Supported providers: Npgsql (PostgreSQL), System.Data.SqlClient / Microsoft.Data.SqlClient (SQL Server).");
                        break;
                }
            }
            catch (Exception e)
            {
                result.Errors.Add($"Error during source database verification: {e}");
            }
        }

        return result;
    }

    private static async Task VerifyPostgreSqlAsync(DbConnection connection, List<string> tableNames, CdcSinkConfiguration configuration, CdcSinkVerificationResult result)
    {
        // Check wal_level = logical
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "SHOW wal_level";
            var walLevel = (await cmd.ExecuteScalarAsync())?.ToString();

            if (string.Equals(walLevel, "logical", StringComparison.OrdinalIgnoreCase) == false)
            {
                result.Errors.Add(
                    $"PostgreSQL wal_level is '{walLevel}', but must be 'logical' for CDC. " +
                    "Set wal_level = logical in postgresql.conf and restart the server.");
            }
        }

        // Check user privileges and existing replication setup
        string currentUser;
        bool hasReplicationPrivilege;
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "SELECT current_user, rolreplication FROM pg_roles WHERE rolname = current_user";
            await using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync() == false)
            {
                result.Errors.Add("Could not determine the current database user's privileges.");
                return;
            }

            currentUser = reader.GetString(0);
            hasReplicationPrivilege = reader.GetBoolean(1);
        }

        if (hasReplicationPrivilege)
        {
            result.HasPermissionToSetup = true;
        }
        else
        {
            // User can't create replication infrastructure — check if admin already set it up.
            // Use the config's Postgres settings if available (user-defined or previously auto-filled),
            // otherwise compute the expected names from the hash.
            var expectedPubName = configuration?.Postgres?.PublicationName
                ?? ComputePublicationName(connection.Database, configuration?.Name ?? "", tableNames);
            var expectedSlotName = configuration?.Postgres?.SlotName
                ?? ComputeSlotName(connection.Database, configuration?.Name ?? "", tableNames);

            bool publicationExists = false;
            bool slotExists = false;

            await using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = "SELECT 1 FROM pg_publication WHERE pubname = @pubName";
                var param = cmd.CreateParameter();
                param.ParameterName = "@pubName";
                param.Value = expectedPubName;
                cmd.Parameters.Add(param);
                publicationExists = await cmd.ExecuteScalarAsync() != null;
            }

            await using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = "SELECT 1 FROM pg_replication_slots WHERE slot_name = @slotName";
                var param = cmd.CreateParameter();
                param.ParameterName = "@slotName";
                param.Value = expectedSlotName;
                cmd.Parameters.Add(param);
                slotExists = await cmd.ExecuteScalarAsync() != null;
            }

            if (publicationExists && slotExists)
            {
                result.Warnings.Add(
                    $"User '{currentUser}' does not have the REPLICATION privilege, but the required publication " +
                    $"'{expectedPubName}' and replication slot '{expectedSlotName}' already exist.");
            }
            else
            {
                var missing = new List<string>();
                var commands = new List<string>();

                if (publicationExists == false)
                {
                    missing.Add($"publication '{expectedPubName}'");
                    var tables = tableNames != null ? string.Join(", ", tableNames) : "ALL TABLES";
                    commands.Add($"CREATE PUBLICATION {expectedPubName} FOR TABLE {tables};");
                }

                if (slotExists == false)
                {
                    missing.Add($"replication slot '{expectedSlotName}'");
                    commands.Add($"SELECT pg_create_logical_replication_slot('{expectedSlotName}', 'pgoutput');");
                }

                result.Errors.Add(
                    $"User '{currentUser}' does not have the REPLICATION privilege and the following are missing: " +
                    $"{string.Join(", ", missing)}. " +
                    $"Either grant the privilege with: ALTER ROLE {currentUser} REPLICATION; " +
                    $"or have an administrator run:\n{string.Join("\n", commands)}");
            }
        }

        // Verify REPLICA IDENTITY for embedded tables whose join columns aren't in the PK.
        // Without FULL or INDEX identity, DELETE events only carry PK columns — insufficient
        // for routing to the parent document when the join column is outside the PK.
        if (configuration != null)
        {
            await VerifyReplicaIdentityForEmbeddedTablesAsync(connection, configuration, result);
        }
    }

    /// <summary>
    /// Checks that each embedded table whose join columns aren't fully covered by its PK
    /// has REPLICA IDENTITY set to FULL or to an INDEX that covers all required columns.
    /// Tables with OnDelete.IgnoreDeletes skip this check.
    /// </summary>
    private static async Task VerifyReplicaIdentityForEmbeddedTablesAsync(
        DbConnection connection, CdcSinkConfiguration configuration, CdcSinkVerificationResult result)
    {
        var tablesToCheck = CollectEmbeddedTablesNeedingReplicaCheck(configuration.Tables);

        foreach (var embedded in tablesToCheck)
        {
            var schema = embedded.SourceTableSchema ?? "public";
            var table = embedded.SourceTableName;
            var joinCols = string.Join(", ", embedded.JoinColumns);
            var requiredColumns = new HashSet<string>(embedded.JoinColumns, StringComparer.OrdinalIgnoreCase);
            foreach (var pk in embedded.PrimaryKeyColumns)
                requiredColumns.Add(pk);

            var error = await CheckReplicaIdentityCoversColumns(connection, schema, table, requiredColumns);
            if (error != null)
            {
                result.Errors.Add(
                    $"Embedded table '{schema}.{table}': {error} " +
                    $"The join column(s) ({joinCols}) are not part of the primary key, so DELETE events " +
                    $"must include them for routing to the parent document. Either:\n\n" +
                    $"  ALTER TABLE {schema}.{table} REPLICA IDENTITY FULL;\n\n" +
                    $"Or set OnDelete.IgnoreDeletes = true on this embedded table configuration to skip delete processing.");
            }
        }

        static List<CdcSinkEmbeddedTableConfig> CollectEmbeddedTablesNeedingReplicaCheck(
            List<CdcSinkTableConfig> rootTables)
        {
            var result = new List<CdcSinkEmbeddedTableConfig>();
            foreach (var root in rootTables)
            {
                CdcSinkConfiguration.ForEachEmbeddedTable(root.EmbeddedTables, e =>
                {
                    if (e.OnDelete?.IgnoreDeletes == true)
                        return;

                    foreach (var joinCol in e.JoinColumns)
                    {
                        if (e.PrimaryKeyColumns.Contains(joinCol) == false)
                        {
                            result.Add(e);
                            return;
                        }
                    }
                });
            }
            return result;
        }
    }

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
            AddParameter(cmd, "@schema", schema);
            AddParameter(cmd, "@table", table);
            var result = await cmd.ExecuteScalarAsync();
            replicaIdentity = result is char c ? c : 'd';
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
                    AddParameter(cmd, "@schema", schema);
                    AddParameter(cmd, "@table", table);
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

        static void AddParameter(DbCommand cmd, string name, object value)
        {
            var param = cmd.CreateParameter();
            param.ParameterName = name;
            param.Value = value;
            cmd.Parameters.Add(param);
        }
    }

    private static async Task VerifySqlServerAsync(DbConnection connection, List<string> tableNames, CdcSinkVerificationResult result)
    {
        // Get database name for error messages
        string dbName;
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "SELECT DB_NAME()";
            dbName = (await cmd.ExecuteScalarAsync())?.ToString() ?? "unknown";
        }

        // Check SQL Server Agent status — required for CDC capture/cleanup jobs
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = @"
                SELECT dss.[status]
                FROM sys.dm_server_services dss
                WHERE dss.[servicename] LIKE N'SQL Server Agent (%'";

            var agentStatus = await cmd.ExecuteScalarAsync();

            // agentStatus = 4 means running. Null means we can't check (insufficient permissions, which is fine).
            if (agentStatus != null && agentStatus != DBNull.Value && Convert.ToInt32(agentStatus) != 4)
            {
                result.Warnings.Add(
                    "SQL Server Agent is not running. CDC capture jobs require SQL Server Agent to be active. " +
                    "Changes will not be captured until the Agent is started.");
            }
        }

        // Check if user has db_owner permissions
        bool hasPermission;
        await using (var permCmd = connection.CreateCommand())
        {
            permCmd.CommandText = "SELECT IS_MEMBER('db_owner')";
            var memberResult = await permCmd.ExecuteScalarAsync();
            hasPermission = memberResult != null && memberResult != DBNull.Value && Convert.ToInt32(memberResult) == 1;
        }

        if (hasPermission)
            result.HasPermissionToSetup = true;

        // Check if CDC is enabled on the database
        bool isCdcEnabled;
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()";
            var cdcResult = await cmd.ExecuteScalarAsync();
            isCdcEnabled = cdcResult != null && cdcResult != DBNull.Value && Convert.ToInt32(cdcResult) == 1;
        }

        if (isCdcEnabled == false)
        {
            if (hasPermission)
            {
                result.Warnings.Add(
                    $"CDC is not enabled on database '{dbName}'. It will be enabled automatically when the task starts.");
            }
            else
            {
                result.Errors.Add(
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
                await using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    var jobType = reader.GetString(reader.GetOrdinal("job_type"));
                    var jobError = reader.IsDBNull(reader.GetOrdinal("job_error"))
                        ? 0
                        : reader.GetInt32(reader.GetOrdinal("job_error"));

                    if (jobError != 0)
                    {
                        result.Warnings.Add(
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

        // Check each configured table has CDC tracking enabled
        if (tableNames == null || tableNames.Count == 0)
            return;

        foreach (var tableName in tableNames)
        {
            // Parse "schema.table" format
            string schema = "dbo";
            string table = tableName;
            var dotIndex = tableName.IndexOf('.');
            if (dotIndex >= 0)
            {
                schema = tableName.Substring(0, dotIndex);
                table = tableName.Substring(dotIndex + 1);
            }

            await using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
                SELECT t.is_tracked_by_cdc
                FROM sys.tables t
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE t.name = @tableName AND s.name = @schemaName";

            var tableParam = cmd.CreateParameter();
            tableParam.ParameterName = "@tableName";
            tableParam.Value = table;
            cmd.Parameters.Add(tableParam);

            var schemaParam = cmd.CreateParameter();
            schemaParam.ParameterName = "@schemaName";
            schemaParam.Value = schema;
            cmd.Parameters.Add(schemaParam);

            var isTracked = await cmd.ExecuteScalarAsync();

            if (isTracked == null || isTracked == DBNull.Value)
            {
                result.Errors.Add($"Table '{schema}.{table}' does not exist in database '{dbName}'.");
            }
            else if (Convert.ToInt32(isTracked) != 1)
            {
                if (hasPermission)
                {
                    result.Warnings.Add(
                        $"CDC tracking is not enabled for table '{schema}.{table}' in database '{dbName}'. " +
                        "It will be enabled automatically when the task starts.");
                }
                else
                {
                    result.Errors.Add(
                        $"CDC tracking is not enabled for table '{schema}.{table}' in database '{dbName}' " +
                        "and the current user does not have db_owner permissions. " +
                        $"Ask a database administrator to run: EXEC sys.sp_cdc_enable_table " +
                        $"@source_schema = '{schema}', @source_name = '{table}', @role_name = NULL;");
                }
            }
        }
    }

    /// <summary>
    /// Computes a hash key for a set of table names, used to track initial-load progress per table.
    /// This is an internal key stored in RavenDB, not a PostgreSQL identifier, so length is unrestricted.
    /// </summary>
    internal static string ComputeTablesHash(List<string> tableNames)
    {
        if (tableNames == null || tableNames.Count == 0)
            return "empty";

        var sorted = string.Join("_", tableNames.OrderBy(t => t));
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(sorted));
        return ToBase32Lower(bytes);
    }

    /// <summary>
    /// Computes the PostgreSQL replication slot name for a given database and set of tables.
    /// Includes the database name in the hash so different databases with the same tables
    /// get distinct slots (pg_replication_slots is a cluster-wide global view).
    /// </summary>
    /// <summary>
    /// Computes the PostgreSQL replication slot name.
    /// Includes the database name and CDC Sink configuration name in the hash so that:
    /// - Different databases with the same tables get distinct slots
    ///   (pg_replication_slots is a cluster-wide global view)
    /// - Two CDC Sink tasks covering the same tables but with different configurations
    ///   get distinct slots (e.g., different column mappings or patches)
    /// </summary>
    internal static string ComputeSlotName(string databaseName, string configName, List<string> tableNames)
    {
        return $"rvn_cdc_s_{ComputeIdentifierHash(databaseName, configName, tableNames)}";
    }

    /// <summary>
    /// Computes the PostgreSQL publication name. Same uniqueness guarantees as the slot name.
    /// </summary>
    internal static string ComputePublicationName(string databaseName, string configName, List<string> tableNames)
    {
        return $"rvn_cdc_p_{ComputeIdentifierHash(databaseName, configName, tableNames)}";
    }

    /// <summary>
    /// Computes a full SHA-256 hash of the database name + config name + sorted table names,
    /// encoded as lowercase base32hex (0-9, a-v). Produces 52 characters for the full 256-bit hash.
    /// Combined with the 10-char prefix ("rvn_cdc_s_" or "rvn_cdc_p_"), the total is 62 chars
    /// — safely under PostgreSQL's 63-char identifier limit (NAMEDATALEN - 1).
    /// </summary>
    private static string ComputeIdentifierHash(string databaseName, string configName, List<string> tableNames)
    {
        var sorted = string.Join("_", tableNames.OrderBy(t => t));
        var input = $"{databaseName}\0{configName}\0{sorted}";
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(input));
        return ToBase32Lower(bytes);
    }

    /// <summary>
    /// Encodes bytes as lowercase base32hex (RFC 4648 §7): digits 0-9 then letters a-v.
    /// No padding. 32 bytes → 52 characters. All characters are valid in unquoted
    /// PostgreSQL identifiers (alphanumeric).
    /// </summary>
    private static string ToBase32Lower(byte[] bytes)
    {
        const string alphabet = "0123456789abcdefghijklmnopqrstuv";
        var sb = new StringBuilder((bytes.Length * 8 + 4) / 5);
        long buffer = 0;
        int bitsLeft = 0;
        for (int i = 0; i < bytes.Length; i++)
        {
            buffer = (buffer << 8) | bytes[i];
            bitsLeft += 8;
            while (bitsLeft >= 5)
            {
                bitsLeft -= 5;
                sb.Append(alphabet[(int)((buffer >> bitsLeft) & 0x1F)]);
            }
        }
        if (bitsLeft > 0)
            sb.Append(alphabet[(int)((buffer << (5 - bitsLeft)) & 0x1F)]);
        return sb.ToString();
    }
}
