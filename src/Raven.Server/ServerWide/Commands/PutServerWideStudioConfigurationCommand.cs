using Raven.Client;
using Raven.Client.ServerWide.Operations.Configuration;
using Sparrow.Json.Parsing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.ServerWide.Commands
{
    public sealed class PutServerWideStudioConfigurationCommand : PutValueCommand<ServerWideStudioConfiguration>
    {
        public PutServerWideStudioConfigurationCommand()
        {
            // for deserialization
        }

        public PutServerWideStudioConfigurationCommand(ServerWideStudioConfiguration value, string uniqueRequestId) : base(uniqueRequestId)
        {
            Name = Constants.Configuration.StudioId;
            Value = value;
        }

        public override DynamicJsonValue ValueToJson()
        {
            return Value?.ToJson();
        }

        public override void AssertBeforeAppend(ClusterOperationContext context, ServerStore serverStore, long index)
        {
            serverStore.Cluster.AssertLicenseLimits(GetType().Name, serverStore, databaseRecord: null, items: null, context);
        }
    }
}
