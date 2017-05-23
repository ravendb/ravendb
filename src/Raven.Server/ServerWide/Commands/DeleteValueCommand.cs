using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class DeleteValueCommand : CommandBase
    {
        public string Name;
        public override DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                ["Type"] = nameof(DeleteValueCommand),
                [nameof(DeleteValueCommand.Name)] = Name,
            };
        }
    }
}
