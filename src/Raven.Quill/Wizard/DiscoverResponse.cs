using System.Linq;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Schema;

namespace Raven.Quill.Wizard;

public sealed class DiscoverResponse
{
    public string? CatalogName { get; set; }

    public required DiscoverTableResponse[] Tables { get; set; } = [];

    public required string[] Errors { get; set; } = [];

    public required bool Success { get; set; }

    public required bool HasPermissionToSetup { get; set; }

    public required string[] Warnings { get; set; } = [];

    internal static DiscoverResponse From(CdcSinkSourceSchema schema) => new()
    {
        CatalogName = schema.CatalogName,
        Tables = schema.Tables.Select(DiscoverTableResponse.From).ToArray(),
        Errors = schema.Errors.ToArray(),
        Success = schema.Success,
        HasPermissionToSetup = schema.HasPermissionToSetup,
        Warnings = schema.Warnings.ToArray(),
    };
}

public sealed class DiscoverTableResponse
{
    public string? SourceTableSchema { get; set; }

    public required string SourceTableName { get; set; } = string.Empty;

    public required DiscoverColumnResponse[] Columns { get; set; } = [];

    public required string[] PrimaryKeyColumns { get; set; } = [];

    public required DiscoverForeignKeyResponse[] ForeignKeys { get; set; } = [];

    public required bool IsCdcEnabled { get; set; }

    public string? UnsupportedReason { get; set; }

    public required string[] Warnings { get; set; } = [];

    internal static DiscoverTableResponse From(CdcSinkSourceTable table) => new()
    {
        SourceTableSchema = table.SourceTableSchema,
        SourceTableName = table.SourceTableName,
        Columns = table.Columns.Select(DiscoverColumnResponse.From).ToArray(),
        PrimaryKeyColumns = table.PrimaryKeyColumns.ToArray(),
        ForeignKeys = table.ForeignKeys.Select(DiscoverForeignKeyResponse.From).ToArray(),
        IsCdcEnabled = table.IsCdcEnabled,
        UnsupportedReason = table.UnsupportedReason,
        Warnings = table.Warnings.ToArray(),
    };
}

public sealed class DiscoverColumnResponse
{
    public required string Name { get; set; } = string.Empty;

    public required string NativeType { get; set; } = string.Empty;

    public required CdcColumnType SuggestedType { get; set; }

    public required bool IsPrimaryKey { get; set; }

    public required bool IsCdcCapturable { get; set; }

    public string? UnsupportedReason { get; set; }

    internal static DiscoverColumnResponse From(CdcSinkSourceColumn column) => new()
    {
        Name = column.Name,
        NativeType = column.NativeType,
        SuggestedType = column.SuggestedType,
        IsPrimaryKey = column.IsPrimaryKey,
        IsCdcCapturable = column.IsCdcCapturable,
        UnsupportedReason = column.UnsupportedReason,
    };
}

public sealed class DiscoverForeignKeyResponse
{
    public required string[] Columns { get; set; } = [];

    public required string ReferencedSchema { get; set; } = string.Empty;

    public required string ReferencedTable { get; set; } = string.Empty;

    public required string[] ReferencedColumns { get; set; } = [];

    internal static DiscoverForeignKeyResponse From(CdcSinkSourceForeignKey foreignKey) => new()
    {
        Columns = foreignKey.Columns.ToArray(),
        ReferencedSchema = foreignKey.ReferencedSchema,
        ReferencedTable = foreignKey.ReferencedTable,
        ReferencedColumns = foreignKey.ReferencedColumns.ToArray(),
    };
}
