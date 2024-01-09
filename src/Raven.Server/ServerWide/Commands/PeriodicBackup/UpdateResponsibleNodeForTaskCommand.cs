using System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.PeriodicBackup;

public class UpdateResponsibleNodeForTaskCommand : UpdateValueForDatabaseCommand
{
    public ResponsibleNodeInfo ResponsibleNodeInfo;

    public const int CommandVersion = 60_001;

    // ReSharper disable once UnusedMember.Local
    private UpdateResponsibleNodeForTaskCommand()
    {
        // for deserialization
    }

    public UpdateResponsibleNodeForTaskCommand(string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
    {
    }

    public override string GetItemId()
    {
        return ResponsibleNodeInfo.GenerateItemName(DatabaseName, ResponsibleNodeInfo.TaskId);
    }

    public override void FillJson(DynamicJsonValue json)
    {
        json[nameof(ResponsibleNodeInfo)] = ResponsibleNodeInfo.ToJson();
    }

    protected override UpdatedValue GetUpdatedValue(long index, RawDatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue)
    {
        return new UpdatedValue(UpdatedValueActionType.Update, context.ReadObject(ResponsibleNodeInfo.ToJson(), GetItemId()));
    }
}

public class ResponsibleNodeInfo : IDynamicJson
{
    public long TaskId { get; set; }

    public string ResponsibleNode { get; set; }

    public DateTime? NotSuitableForTaskSince { get; set; }

    public static string GenerateItemName(string databaseName, long taskId)
    {
        return $"values/{databaseName}/responsible-node/{taskId}";
    }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(TaskId)] = TaskId,
            [nameof(ResponsibleNode)] = ResponsibleNode,
            [nameof(NotSuitableForTaskSince)] = NotSuitableForTaskSince
        };
    }
}
