using Raven.Client.Documents.Operations.ETL.SQL;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.CdcSink.Test;

/// <summary>
/// Body of <c>POST /admin/cdc-sink/test</c>. Drives a synthetic CDC mapping run against
/// one or more rows pulled from the configured source table — used by the Studio "Test"
/// button and by .NET clients that want to preview how rows will become documents.
/// </summary>
internal class TestCdcSinkMappingRequest : IDynamicJson
{
    /// <summary>
    /// The full CDC task configuration — same JSON Studio uses to create/update a CDC task.
    /// Driver dispatch, target-table lookup, column mapping, and patch scripts all come from here.
    /// </summary>
    public CdcSinkConfiguration Configuration { get; set; }

    /// <summary>
    /// Inline credentials. Required path for Studio's Task Creation view, where the user
    /// is editing the connection but hasn't saved it to <c>databaseRecord.SqlConnectionStrings</c>
    /// yet. When null, falls back to <see cref="CdcSinkConfiguration.ConnectionStringName"/>.
    /// </summary>
    public SqlConnectionString Connection { get; set; }

    public string SourceTableSchema { get; set; }

    public string SourceTableName { get; set; }

    public TestCdcSinkRowSelector RowSelector { get; set; }

    /// <summary>Primary-key values, in the same order as the target table's <c>PrimaryKeyColumns</c>. Required when <see cref="RowSelector"/> is <c>ByPrimaryKey</c>.</summary>
    public string[] PrimaryKeyValues { get; set; }

    public TestCdcSinkOperation Operation { get; set; }

    /// <summary>
    /// Number of rows to fetch and mutate. Only meaningful with <see cref="TestCdcSinkRowSelector.First"/>;
    /// must be 1 when <see cref="RowSelector"/> is <c>ByPrimaryKey</c>. Defaults to 1.
    /// Capped at 5,000 server-side — the endpoint is a preview helper, not a bulk-fetch path;
    /// streaming larger samples is a planned follow-up.
    /// </summary>
    public int MaxRows { get; set; } = 1;

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Configuration)] = Configuration?.ToJson(),
            [nameof(Connection)] = Connection?.ToJson(),
            [nameof(SourceTableSchema)] = SourceTableSchema,
            [nameof(SourceTableName)] = SourceTableName,
            [nameof(RowSelector)] = RowSelector.ToString(),
            [nameof(PrimaryKeyValues)] = PrimaryKeyValues == null ? null : new DynamicJsonArray(PrimaryKeyValues),
            [nameof(Operation)] = Operation.ToString(),
            [nameof(MaxRows)] = MaxRows,
        };
    }
}
