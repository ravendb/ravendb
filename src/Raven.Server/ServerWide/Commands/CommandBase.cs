using System;
using Raven.Client.Exceptions.Security;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
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
                throw new RachisApplyException("Command must contain 'Type' field.");
            }

            if (JsonDeserializationCluster.Commands.TryGetValue(type, out Func<BlittableJsonReaderObject, CommandBase> deserializer) == false)
                throw new InvalidOperationException($"There is not deserializer for '{type}' command.");

            return deserializer(json);
        }

        public virtual void VerifyCanExecuteCommand(ServerStore store, TransactionOperationContext context, bool isClusterAdmin)
        {
            // sub classes can assert what their required clearance
            // should be to execute this command, the minimum level
            // is operator
        }

        protected void AssertClusterAdmin(bool isClusterAdmin)
        {
            if (isClusterAdmin)
                return;

            throw new AuthorizationException($"Attempted to {GetType().Name} but this is only available for cluster administrators");
        }

        public virtual object FromRemote(object remoteResult)
        {
            return remoteResult;
        }
    }
}
