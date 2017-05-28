using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class PutValueCommand : CommandBase
    {
        public string Name;
        public BlittableJsonReaderObject Value;
        public override DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                ["Type"] = nameof(PutValueCommand),
                [nameof(PutValueCommand.Name)] = Name,
                [nameof(PutValueCommand.Value)] = Value
            };
        }
    }
}
