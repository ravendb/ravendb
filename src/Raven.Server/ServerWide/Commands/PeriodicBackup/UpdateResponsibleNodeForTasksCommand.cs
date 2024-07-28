using System;
using System.Collections.Generic;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.PeriodicBackup;

public class UpdateResponsibleNodeForTasksCommand : UpdateValueCommand<UpdateResponsibleNodeForTasksCommand.Parameters>
{
    public const int CommandVersion = 60_001;

    // ReSharper disable once UnusedMember.Local
    private UpdateResponsibleNodeForTasksCommand()
    {
        // for deserialization
    }

    public UpdateResponsibleNodeForTasksCommand(Parameters parameters, string uniqueRequestId) : base(uniqueRequestId)
    {
        Value = parameters;
    }

    public override object ValueToJson()
    {
        return Value.ToJson();
    }

    public override BlittableJsonReaderObject GetUpdatedValue(JsonOperationContext context, BlittableJsonReaderObject previousValue, long index)
    {
        return null;
    }

    public class Parameters : IDynamicJson
    {
        public Dictionary<string, List<ResponsibleNodeInfo>> ResponsibleNodePerDatabase { get; set; }

        public DynamicJsonValue ToJson()
        {
            var djv = new DynamicJsonValue();

            foreach (var keyValue in ResponsibleNodePerDatabase)
            {
                var list = new DynamicJsonArray();
                foreach (var responsibleNodeInfo in keyValue.Value)
                {
                    list.Add(responsibleNodeInfo.ToJson());
                }

                djv[keyValue.Key] = list;
            }

            return new DynamicJsonValue
            {
                [nameof(ResponsibleNodePerDatabase)] = djv
            };
        }
    }
}

public class ResponsibleNodeInfo : IDynamicJson
{
    public long TaskId { get; set; }

    public string ResponsibleNode { get; set; }

    public DateTime? NotSuitableForTaskSince { get; set; }

    public static string GenerateItemName(string databaseName, long taskId)
    {
        return $"{GetPrefix(databaseName)}{taskId}";
    }

    public static string GetPrefix(string databaseName)
    {
        return $"values/{databaseName}/responsible-node/";
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
