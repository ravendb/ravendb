using System;
using System.Data.Common;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.ETL.SQL;

namespace Raven.Server.Documents.CdcSink;

/// <summary>
/// Verifies that a source SQL database is properly configured for CDC (Change Data Capture).
/// For PostgreSQL: checks wal_level=logical and user has REPLICATION privilege.
/// For SQL Server: checks CDC is enabled on the database and all configured tables.
/// </summary>
public static class CdcSinkSourceVerifier
{
    public static async Task<CdcSinkVerificationResult> VerifyAsync(SqlConnectionString connection, CdcSinkConfiguration configuration)
    {
        var result = new CdcSinkVerificationResult();

        DbProviderFactory factory;
        try
        {
            factory = DbProviderFactories.GetFactory(connection.FactoryName);
        }
        catch (Exception e)
        {
            result.Errors.Add($"Could not find database provider factory '{connection.FactoryName}': {e.Message}");
            return result;
        }

        await using var dbConnection = factory.CreateConnection();
        if (dbConnection == null)
        {
            result.Errors.Add($"Could not create connection using factory '{connection.FactoryName}'");
            return result;
        }

        dbConnection.ConnectionString = connection.ConnectionString;

        try
        {
            await dbConnection.OpenAsync();
        }
        catch (Exception e)
        {
            result.Errors.Add($"Could not connect to source database: {e.Message}");
            return result;
        }

        try
        {
            switch (connection.FactoryName)
            {
                case "Npgsql":
                    await VerifyPostgreSqlAsync(dbConnection, configuration, result);
                    break;

                case "System.Data.SqlClient":
                case "Microsoft.Data.SqlClient":
                    await VerifySqlServerAsync(dbConnection, configuration, result);
                    break;

                default:
                    result.Warnings.Add($"CDC source verification is not supported for provider '{connection.FactoryName}'. " +
                                        "Please ensure your database is configured for CDC manually.");
                    break;
            }
        }
        finally
        {
            await dbConnection.CloseAsync();
        }

        return result;
    }

    private static async Task VerifyPostgreSqlAsync(DbConnection connection, CdcSinkConfiguration configuration, CdcSinkVerificationResult result)
    {
        // 1. Check wal_level = logical
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

        // 2. Check user has REPLICATION privilege
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "SELECT rolreplication FROM pg_roles WHERE rolname = current_user";
            var rolReplication = await cmd.ExecuteScalarAsync();

            if (rolReplication == null || rolReplication == DBNull.Value || !(bool)rolReplication)
            {
                result.Errors.Add(
                    "The current database user does not have the REPLICATION privilege. " +
                    "Grant it with: ALTER ROLE <username> REPLICATION;");
            }
        }

        // 3. Verify configured tables exist
        foreach (var table in configuration.Tables)
        {
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = @schema AND table_name = @table";

            var schemaParam = cmd.CreateParameter();
            schemaParam.ParameterName = "@schema";
            schemaParam.Value = table.SourceTableSchema ?? "public";
            cmd.Parameters.Add(schemaParam);

            var tableParam = cmd.CreateParameter();
            tableParam.ParameterName = "@table";
            tableParam.Value = table.SourceTableName;
            cmd.Parameters.Add(tableParam);

            var count = Convert.ToInt32(await cmd.ExecuteScalarAsync());
            if (count == 0)
            {
                result.Errors.Add(
                    $"Table '{table.SourceTableSchema}.{table.SourceTableName}' does not exist in the source database.");
            }
        }
    }

    private static async Task VerifySqlServerAsync(DbConnection connection, CdcSinkConfiguration configuration, CdcSinkVerificationResult result)
    {
        // 1. Check CDC is enabled on the database
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()";
            var isCdcEnabled = await cmd.ExecuteScalarAsync();

            if (isCdcEnabled == null || isCdcEnabled == DBNull.Value || Convert.ToInt32(isCdcEnabled) != 1)
            {
                result.Errors.Add(
                    "CDC is not enabled on the source database. " +
                    "Enable it with: EXEC sys.sp_cdc_enable_db;");
                return; // No point checking tables if DB CDC is not enabled
            }
        }

        // 2. Check each configured table has CDC tracking enabled
        foreach (var table in configuration.Tables)
        {
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
                SELECT t.is_tracked_by_cdc
                FROM sys.tables t
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE t.name = @tableName AND s.name = @schemaName";

            var tableParam = cmd.CreateParameter();
            tableParam.ParameterName = "@tableName";
            tableParam.Value = table.SourceTableName;
            cmd.Parameters.Add(tableParam);

            var schemaParam = cmd.CreateParameter();
            schemaParam.ParameterName = "@schemaName";
            schemaParam.Value = table.SourceTableSchema ?? "dbo";
            cmd.Parameters.Add(schemaParam);

            var isTracked = await cmd.ExecuteScalarAsync();

            if (isTracked == null || isTracked == DBNull.Value)
            {
                result.Errors.Add(
                    $"Table '{table.SourceTableSchema}.{table.SourceTableName}' does not exist in the source database.");
            }
            else if (Convert.ToInt32(isTracked) != 1)
            {
                result.Errors.Add(
                    $"CDC tracking is not enabled for table '{table.SourceTableSchema}.{table.SourceTableName}'. " +
                    $"Enable it with: EXEC sys.sp_cdc_enable_table @source_schema = '{table.SourceTableSchema}', " +
                    $"@source_name = '{table.SourceTableName}', @role_name = NULL;");
            }
        }
    }
}
