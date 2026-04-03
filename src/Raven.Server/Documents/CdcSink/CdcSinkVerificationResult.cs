using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.CdcSink;

/// <summary>
/// Result of verifying a source database is configured for CDC.
/// </summary>
public class CdcSinkVerificationResult
{
    public bool Success => Errors.Count == 0;

    /// <summary>
    /// Whether the current user has sufficient permissions to create the CDC
    /// infrastructure (replication slot/publication for PostgreSQL, enable CDC for SQL Server).
    /// When false, an administrator must set it up.
    /// </summary>
    public bool HasPermissionToSetup { get; set; }

    public List<string> Errors { get; } = new();

    public List<string> Warnings { get; } = new();

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Success)] = Success,
            [nameof(HasPermissionToSetup)] = HasPermissionToSetup,
            [nameof(Errors)] = new DynamicJsonArray(Errors),
            [nameof(Warnings)] = new DynamicJsonArray(Warnings),
        };
    }
}
