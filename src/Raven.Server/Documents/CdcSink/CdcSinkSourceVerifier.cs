using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.ETL.SQL;

namespace Raven.Server.Documents.CdcSink;

/// <summary>
/// Verifies that a source SQL database is properly configured for CDC.
/// Checks both whether the user has permissions to set up CDC, and whether
/// CDC is already configured (for reduced-privilege scenarios where an admin
/// set it up separately).
/// </summary>
public static class CdcSinkSourceVerifier
{
    public static async Task<CdcSinkVerificationResult> VerifyAsync(SqlConnectionString connection, List<string> tableNames = null)
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
                        await VerifyPostgreSqlAsync(dbConnection, tableNames, result);
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

    private static async Task VerifyPostgreSqlAsync(DbConnection connection, List<string> tableNames, CdcSinkVerificationResult result)
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
            // User can't create replication infrastructure — check if admin already set it up
            var expectedPubName = ComputePublicationName(tableNames);
            var expectedSlotName = ComputeSlotName(tableNames);

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

    internal static string ComputeTablesHash(List<string> tableNames)
    {
        if (tableNames == null || tableNames.Count == 0)
            return "empty";

        var sorted = string.Join("_", tableNames.OrderBy(t => t));
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(sorted));
        return Convert.ToHexString(bytes)[..16].ToLower();
    }

    internal static string ComputeSlotName(List<string> tableNames)
    {
        return $"rvn_cdc_slot_{ComputeTablesHash(tableNames)}";
    }

    internal static string ComputePublicationName(List<string> tableNames)
    {
        return $"rvn_cdc_pub_{ComputeTablesHash(tableNames)}";
    }
}
