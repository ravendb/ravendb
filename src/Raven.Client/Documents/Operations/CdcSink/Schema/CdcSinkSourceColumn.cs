using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.CdcSink.Schema;

/// <summary>
/// One source-side column as the CDC schema-discovery endpoint sees it. Field
/// names mirror <see cref="CdcColumnMapping"/> so Studio can drop the response
/// straight into its mapping model.
/// </summary>
public class CdcSinkSourceColumn : IDynamicJson
{
    /// <summary>
    /// Source column name. Matches <see cref="CdcColumnMapping.Column"/>.
    /// </summary>
    public string Name { get; set; }

    /// <summary>
    /// Raw source-side type string (e.g. <c>"varchar"</c>, <c>"bigint"</c>, <c>"jsonb"</c>).
    /// For MySQL this is DATA_TYPE (without precision); the fuller COLUMN_TYPE form is not
    /// surfaced here today.
    /// </summary>
    public string NativeType { get; set; }

    /// <summary>
    /// What CDC will store this column as by default. <c>Json</c> for PostgreSQL <c>jsonb</c>/<c>json</c>
    /// and MySQL <c>json</c>; <c>Attachment</c> for PostgreSQL <c>bytea</c> and SQL Server
    /// <c>varbinary</c>/<c>image</c>; <c>Default</c> otherwise.
    /// </summary>
    public CdcColumnType SuggestedType { get; set; }

    public bool IsPrimaryKey { get; set; }

    /// <summary>
    /// True when CDC can actually capture this column. False when the source-side type
    /// has no CDC mapping (PostgreSQL extension OIDs the streaming code would throw on)
    /// or when the column is not enrolled in the SQL Server CDC capture list.
    /// </summary>
    public bool IsCdcCapturable { get; set; }

    /// <summary>
    /// Human-readable reason set when <see cref="IsCdcCapturable"/> is false.
    /// Null on capturable columns.
    /// </summary>
    public string UnsupportedReason { get; set; }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Name)] = Name,
            [nameof(NativeType)] = NativeType,
            [nameof(SuggestedType)] = SuggestedType.ToString(),
            [nameof(IsPrimaryKey)] = IsPrimaryKey,
            [nameof(IsCdcCapturable)] = IsCdcCapturable,
            [nameof(UnsupportedReason)] = UnsupportedReason,
        };
    }
}
