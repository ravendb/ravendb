using System;
using Raven.Client.Server;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public abstract class UpdateDatabaseCommand : CommandBase
    {
        public string DatabaseName;

        protected UpdateDatabaseCommand(string databaseName)
        {
            DatabaseName = databaseName;
        }

        public abstract string UpdateDatabaseRecord(DatabaseRecord record, long etag);

        public abstract void FillJson(DynamicJsonValue json);

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var djv = base.ToJson(context);
            djv[nameof(DatabaseName)] = DatabaseName;

            FillJson(djv);

            return djv;
        }
    }

    public abstract class CommandBase
    {
        public virtual DynamicJsonValue ToJson(JsonOperationContext context)
        {
            return new DynamicJsonValue
            {
                ["Type"] = GetType().Name
            };
        }

        public long? Etag;

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