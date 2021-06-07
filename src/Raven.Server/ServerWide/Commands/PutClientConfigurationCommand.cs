using Raven.Client;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class PutClientConfigurationCommand : PutValueCommand<ClientConfiguration>
    {
        public PutClientConfigurationCommand()
        {
            // for deserialization
        }

        public PutClientConfigurationCommand(ClientConfiguration value, string uniqueRequestId) : base(uniqueRequestId)
        {
            Name = Constants.Configuration.ClientId;
            Value = value;
        }

        public override void UpdateValue(ClusterOperationContext context, long index)
        {
            Value.Etag = index;
        }

        public override DynamicJsonValue ValueToJson()
        {
            return Value?.ToJson();
        }
    }
}
