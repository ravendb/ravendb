using System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public abstract class CommandBase
    {
        public virtual DynamicJsonValue ToJson(JsonOperationContext context)
        {
            return new DynamicJsonValue
            {
                ["Type"] = GetType().Name
            };
        }

        public long? RaftCommandIndex;

        public static CommandBase CreateFrom(BlittableJsonReaderObject json)
        {
            if (json.TryGet("Type", out string type) == false)
            {
                // TODO: maybe add further validation?
                throw new ArgumentException("Command must contain 'Type' field.");
            }

            if (JsonDeserializationCluster.Commands.TryGetValue(type, out Func<BlittableJsonReaderObject, CommandBase> deserializer) == false)
                throw new InvalidOperationException($"There is not deserializer for '{type}' command.");

            return deserializer(json);
        }
    }
}