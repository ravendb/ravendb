using System;
using System.Text.RegularExpressions;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Client.Documents.Operations.CdcSink.Test;
using Raven.Client.Documents.Operations.ETL.SQL;

namespace Raven.Server.Documents.CdcSink.Handlers;

/// <summary>
/// Shared validation + connection-resolution helpers used by the CDC sink admin endpoints.
/// </summary>
internal static class CdcSinkRequestValidation
{
    /// <summary>
    /// Hard cap on the number of rows the test-mapping endpoint will fetch + materialise + run
    /// scripts against in a single request. The endpoint buffers the full result set in memory
    /// and writes it as one JSON response, so this cap also bounds the worst-case response size.
    /// </summary>
    internal const int MaxAllowedTestRows = 5000;

    /// <summary>
    /// Identifier shape gate. Callers interpolate user-supplied identifiers into raw SQL
    /// (the migrator's <c>QuoteTable</c> / <c>QuoteColumn</c> for table and column names, and
    /// Postgres' <c>INFORMATION_SCHEMA</c> filter for schema names). Reject anything outside the
    /// standard SQL identifier shape so a typo or malicious value can't break out of the quoted context.
    /// </summary>
    private static readonly Regex IdentifierPattern = new("^[A-Za-z_][A-Za-z0-9_]*$", RegexOptions.Compiled);

    internal static bool TryValidateIdentifier(string value, string fieldName, CdcSinkSourceSchema schemaResult, TestCdcSinkMappingResult testResult, bool allowEmpty = true)
    {
        // Schema fields can legitimately be empty (default-schema fallback handles it). For
        // table / PK / column identifiers, empty would flow into ORDER BY / WHERE generation
        // as a SQL syntax error - callers in those positions pass allowEmpty: false.
        if (string.IsNullOrEmpty(value))
        {
            if (allowEmpty)
                return true;
            var emptyError = $"'{fieldName}' must not be empty.";
            if (schemaResult != null)
                schemaResult.Errors.Add(emptyError);
            if (testResult != null)
                testResult.Errors.Add(emptyError);
            return false;
        }
        if (IdentifierPattern.IsMatch(value))
            return true;
        var error = $"'{fieldName}' value '{value}' contains invalid characters. Use letters, digits, and underscores only.";
        if (schemaResult != null)
            schemaResult.Errors.Add(error);
        if (testResult != null)
            testResult.Errors.Add(error);
        return false;
    }

    /// <summary>
    /// Common inline-vs-named connection-string resolver for the CDC admin endpoints.
    /// Inline <paramref name="inline"/> (a fully-populated <see cref="SqlConnectionString"/>)
    /// wins when present; otherwise <paramref name="connectionStringName"/> is looked up in
    /// <c>databaseRecord.SqlConnectionStrings</c>. The two field-name parameters only affect
    /// the error-message text - each endpoint shows the property name its own request DTO uses.
    /// </summary>
    internal static SqlConnectionString ResolveSqlConnection(
        DocumentDatabase database,
        SqlConnectionString inline,
        string connectionStringName,
        string inlineFieldName,
        string namedFieldName)
    {
        if (inline != null
            && string.IsNullOrEmpty(inline.FactoryName) == false
            && string.IsNullOrEmpty(inline.ConnectionString) == false)
        {
            return inline;
        }

        if (string.IsNullOrEmpty(connectionStringName))
            throw new InvalidOperationException(
                $"Provide either '{inlineFieldName}' (inline {nameof(SqlConnectionString.FactoryName)} + {nameof(SqlConnectionString.ConnectionString)}) " +
                $"or '{namedFieldName}'.");

        var databaseRecord = database.ReadDatabaseRecord();
        if (databaseRecord.SqlConnectionStrings.TryGetValue(connectionStringName, out var named) == false)
            throw new InvalidOperationException($"SQL connection string '{connectionStringName}' was not found in the database configuration.");

        return named;
    }
}
