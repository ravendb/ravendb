using System;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Client.ServerWide.Operations.OngoingTasks;
using Raven.Server.Rachis;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class ToggleServerWideTaskStateCommand : UpdateValueCommand<ToggleServerWideTaskStateCommand.Parameters>
    {
        protected ToggleServerWideTaskStateCommand()
        {
            // for deserialization
        }

        public ToggleServerWideTaskStateCommand(Parameters configuration, string uniqueRequestId) : base(uniqueRequestId)
        {
            Name = ClusterStateMachine.ServerWideConfigurationKey.GetKeyByType(configuration.Type);
            Value = configuration;
        }

        public override object ValueToJson()
        {
            return Value.ToJson();
        }

        public override BlittableJsonReaderObject GetUpdatedValue(JsonOperationContext context, BlittableJsonReaderObject previousValue, long index)
        {
            if (previousValue == null)
                throw new RachisApplyException($"Cannot find any server wide tasks of type '{Value.Type}'");

            if (previousValue.TryGet(Value.TaskName, out BlittableJsonReaderObject task) == false)
                throw new RachisApplyException($"Cannot find server wide task of type '{Value.Type}' with name '{Value.TaskName}'");

            if (task.Modifications == null)
                task.Modifications = new DynamicJsonValue();

            task.Modifications = new DynamicJsonValue
            {
                [GetDisabledPropertyName()] = Value.Disable
            };

            return context.ReadObject(previousValue, Name);
        }

        private string GetDisabledPropertyName()
        {
            switch (Value.Type)
            {
                case OngoingTaskType.Backup:
                    return nameof(ServerWideBackupConfiguration.Disabled);
                case OngoingTaskType.Replication:
                    return nameof(ServerWideExternalReplication.Disabled);
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public class Parameters : IDynamicJson
        {
            public OngoingTaskType Type { get; set; }

            public string TaskName { get; set; }

            public bool Disable { get; set; }

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(Type)] = Type,
                    [nameof(TaskName)] = TaskName,
                    [nameof(Disable)] = Disable
                };
            }
        }
    }
}
