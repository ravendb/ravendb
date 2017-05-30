using Raven.Client.Server.Operations.ApiKeys;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class PutApiKeyCommand : PutValueCommand<ApiKeyDefinition>
    {
        public PutApiKeyCommand()
        {
            // for deserialization
        }

        public PutApiKeyCommand(string name, ApiKeyDefinition value)
        {
            Name = name;
            Value = value;
        }

        public override DynamicJsonValue ValueToJson()
        {
            return Value?.ToJson();
        }
    }
}