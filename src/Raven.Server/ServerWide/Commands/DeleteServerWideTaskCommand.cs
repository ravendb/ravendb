using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class DeleteServerWideTaskCommand : UpdateValueCommand<DeleteServerWideTaskCommand.DeleteConfiguration>
    {
        protected DeleteServerWideTaskCommand()
        {
            // for deserialization
        }

        public DeleteServerWideTaskCommand(DeleteConfiguration configuration, string uniqueRequestId) : base(uniqueRequestId)
        {
            Name = ClusterStateMachine.ServerWideConfigurationKey.GetKeyByType(configuration.Type);
            Value = configuration;
        }

        public static string GetDatabaseRecordTaskName(DeleteConfiguration configuration)
        {
            switch (configuration.Type)
            {
                case OngoingTaskType.Backup:
                    return PutServerWideBackupConfigurationCommand.GetTaskName(configuration.TaskName);
                case OngoingTaskType.Replication:
                    return PutServerWideExternalReplicationCommand.GetTaskName(configuration.TaskName);
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public override object ValueToJson()
        {
            return Value.ToJson();
        }

        public override BlittableJsonReaderObject GetUpdatedValue(JsonOperationContext context, BlittableJsonReaderObject previousValue, long index)
        {
            if (previousValue == null)
                return null;

            var propertyIndex = previousValue.GetPropertyIndex(Value.TaskName);
            if (propertyIndex == -1)
                return null;

            if (previousValue.Modifications == null)
                previousValue.Modifications = new DynamicJsonValue();

            previousValue.Modifications.Removals = new HashSet<int> { propertyIndex };
            return context.ReadObject(previousValue, Name);
        }

        public override void VerifyCanExecuteCommand(ServerStore store, TransactionOperationContext context, bool isClusterAdmin)
        {
            AssertClusterAdmin(isClusterAdmin);
        }

        public class DeleteConfiguration : IDynamicJson
        {
            public OngoingTaskType Type { get; set; }

            public string TaskName { get; set; }

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(Type)] = Type,
                    [nameof(TaskName)] = TaskName
                };
            }
        }
    }
}
