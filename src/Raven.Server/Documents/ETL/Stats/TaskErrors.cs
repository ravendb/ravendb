using System.Linq;
using Raven.Client.Documents.Operations.ETL;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Stats;

public sealed class TaskErrors : IDynamicJson
{
    public string TaskName { get; set; }

    public EtlType? EtlType { get; set; }

    public string EtlSubType { get; set; }

    public TaskProcessError[] ProcessErrors { get; set; }

    public TaskItemError[] ItemErrors { get; set; }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(TaskName)] = TaskName,
            [nameof(EtlType)] = EtlType?.ToString(),
            [nameof(EtlSubType)] = EtlSubType,
            [nameof(ProcessErrors)] = new DynamicJsonArray(ProcessErrors.Select(x => x.ToJson())),
            [nameof(ItemErrors)] = new DynamicJsonArray(ItemErrors.Select(x => x.ToJson()))
        };
    }
}
