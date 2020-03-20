using System;
using System.Collections.Generic;
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
            Name = GetName();
            Value = configuration;

            string GetName()
            {
                switch (configuration.Type)
                {
                    case DeleteConfiguration.TaskType.Backup:
                        return ClusterStateMachine.ServerWideConfigurationKey.Backup;
                    case DeleteConfiguration.TaskType.ExternalReplication:
                        return ClusterStateMachine.ServerWideConfigurationKey.ExternalReplication;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
        }

        public static string GetDatabaseRecordTaskName(DeleteConfiguration configuration)
        {
            switch (configuration.Type)
            {
                case DeleteConfiguration.TaskType.Backup:
                    return PutServerWideBackupConfigurationCommand.GetTaskName(configuration.Name);
                case DeleteConfiguration.TaskType.ExternalReplication:
                    return PutServerWideExternalReplicationCommand.GetTaskName(configuration.Name);
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

            var propertyIndex = previousValue.GetPropertyIndex(Value.Name);
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
            public string Name { get; set; }

            public TaskType Type { get; set; }

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(Name)] = Name,
                    [nameof(Type)] = Type
                };
            }

            public enum TaskType
            {
                Backup,
                ExternalReplication
            }
        }
    }
}
