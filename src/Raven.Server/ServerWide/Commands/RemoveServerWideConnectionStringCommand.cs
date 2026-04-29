using System.Collections.Generic;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public sealed class RemoveServerWideConnectionStringCommand : UpdateValueCommand<RemoveServerWideConnectionStringCommand.DeleteConfiguration>
    {
        public RemoveServerWideConnectionStringCommand()
        {
            // for deserialization
        }

        public RemoveServerWideConnectionStringCommand(DeleteConfiguration configuration, string uniqueRequestId) : base(uniqueRequestId)
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
                return null;

            var propertyIndex = previousValue.GetPropertyIndex(Value.ConnectionStringName);
            if (propertyIndex == -1)
                return null;

            previousValue.Modifications ??= new DynamicJsonValue();

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
