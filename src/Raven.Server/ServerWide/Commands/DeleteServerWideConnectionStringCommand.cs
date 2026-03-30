using System.Collections.Generic;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public sealed class DeleteServerWideConnectionStringCommand : UpdateValueCommand<DeleteServerWideConnectionStringCommand.DeleteConfiguration>
    {
        public DeleteServerWideConnectionStringCommand()
        {
            // for deserialization
        }

        public DeleteServerWideConnectionStringCommand(DeleteConfiguration configuration, string uniqueRequestId) : base(uniqueRequestId)
        {
            Name = ClusterStateMachine.ServerWideConfigurationKey.GetConnectionStringKeyByType(configuration.Type);
            Value = configuration;
        }

        public override object ValueToJson()
        {
            return Value.ToJson();
        }

        public override BlittableJsonReaderObject GetUpdatedValue(JsonOperationContext context, BlittableJsonReaderObject previousValue, long index)
        {
            if (previousValue == null)
                throw new RachisInvalidOperationException(
                    "There are no server-wide connection strings so nothing to delete: " +
                    $"raftIndex {index}, configuration {context.ReadObject(Value.ToJson(), "")}");

            var propertyIndex = previousValue.GetPropertyIndex(Value.ConnectionStringName);
            if (propertyIndex == -1)
                throw new RachisInvalidOperationException(
                    "The server-wide connection string to delete doesn't exist: " +
                    $"raftIndex {index}, previousValue {previousValue}, configuration {context.ReadObject(Value.ToJson(), "")}");

            if (previousValue.Modifications == null)
                previousValue.Modifications = new DynamicJsonValue();

            previousValue.Modifications.Removals = new HashSet<int> { propertyIndex };
            return context.ReadObject(previousValue, Name);
        }

        public override void VerifyCanExecuteCommand(ServerStore store, TransactionOperationContext context, bool isClusterAdmin)
        {
            AssertClusterAdmin(isClusterAdmin);
        }

        public sealed class DeleteConfiguration : IDynamicJson
        {
            public ConnectionStringType Type { get; set; }

            public string ConnectionStringName { get; set; }

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(Type)] = Type,
                    [nameof(ConnectionStringName)] = ConnectionStringName
                };
            }
        }
    }
}
