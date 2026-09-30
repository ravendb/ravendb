using Raven.Client.Documents.Operations.ETL.SQL;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.CdcSink.Test;

internal class CdcTestRequest : IDynamicJson
{
    public CdcSinkConfiguration Configuration { get; set; }

    public SqlConnectionString Connection { get; set; }

    public DynamicJsonValue ToJson() => new()
    {
        [nameof(Configuration)] = Configuration?.ToJson(),
        [nameof(Connection)] = Connection?.ToJson(),
    };
}
