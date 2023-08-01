using Raven.Client;
using Raven.Client.ServerWide.Operations.Configuration;
using Sparrow.Json.Parsing;

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
    }
}
