using System;
using System.Linq;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class PutServerWideExternalReplicationCommand : UpdateValueCommand<ServerWideExternalReplication>
    {
        protected PutServerWideExternalReplicationCommand()
        {
            // for deserialization
        }

        public PutServerWideExternalReplicationCommand(ServerWideExternalReplication configuration, string uniqueRequestId) : base(uniqueRequestId)
        {
            Name = ClusterStateMachine.ServerWideConfigurationKey.ExternalReplication;
            Value = configuration;
        }

        public override object ValueToJson()
        {
            return Value.ToJson();
        }

        public override BlittableJsonReaderObject GetUpdatedValue(JsonOperationContext context, BlittableJsonReaderObject previousValue, long index)
        {
            if (string.IsNullOrWhiteSpace(Value.Name))
            {
                Value.Name = GenerateTaskName(previousValue);
            }

            Value.TaskId = index;

            if (previousValue != null)
            {
                if (previousValue.Modifications == null)
                    previousValue.Modifications = new DynamicJsonValue();

                previousValue.Modifications = new DynamicJsonValue
                {
                    [Value.Name] = Value.ToJson()
                };

                return context.ReadObject(previousValue, Name);
            }

            var djv = new DynamicJsonValue
            {
                [Value.Name] = Value.ToJson()
            };

            return context.ReadObject(djv, Name);
        }

        private string GenerateTaskName(BlittableJsonReaderObject previousValue)
        {
            var baseTaskName = Value.GetDefaultTaskName();
            if (previousValue == null)
                return baseTaskName;

            long i = 1;
            var taskName = baseTaskName;
            var allTaskNames = previousValue.GetPropertyNames();
            while (allTaskNames.Contains(taskName, StringComparer.OrdinalIgnoreCase))
            {
                taskName += $" #{++i}";
            }

            return taskName;
        }

        public override void VerifyCanExecuteCommand(ServerStore store, TransactionOperationContext context, bool isClusterAdmin)
        {
            AssertClusterAdmin(isClusterAdmin);
        }

        public static string GetTaskName(string serverWideName)
        {
            return $"{ServerWideExternalReplication.NamePrefix}, {serverWideName}";
        }

        public static string GetRavenConnectionStringName(string serverWideName)
        {
            return $"{ServerWideExternalReplication.RavenConnectionStringPrefix} for {serverWideName}";
        }

        public static RavenConnectionString UpdateExternalReplicationTemplateForDatabase(ExternalReplication configuration, string databaseName, string[] topologyDiscoveryUrls)
        {
            var serverWideName = configuration.Name;
            configuration.Name = GetTaskName(serverWideName);
            configuration.Database = databaseName;
            configuration.ConnectionStringName = GetRavenConnectionStringName(serverWideName);

            return new RavenConnectionString
            {
                Name = configuration.ConnectionStringName,
                Database = databaseName,
                TopologyDiscoveryUrls = topologyDiscoveryUrls
            };
        }
    }
}
