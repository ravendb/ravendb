using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.PeriodicBackup.BackupHistory;

public sealed class BackupHistoryUnreachableNode : IDynamicJson
{
    public string NodeTag { get; set; }
    public string Error { get; set; }

    public DynamicJsonValue ToJson() =>
        new()
        {
            [nameof(NodeTag)] = NodeTag,
            [nameof(Error)] = Error
        };
}
