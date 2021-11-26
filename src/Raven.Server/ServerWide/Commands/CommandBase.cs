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
            var json = new DynamicJsonValue
            {
                ["Type"] = GetType().Name,
                [nameof(UniqueRequestId)] = UniqueRequestId,
            };

            if (Timeout.HasValue)
                json[nameof(Timeout)] = Timeout;

            return json;
        }

        public TimeSpan? Timeout;

        public long? RaftCommandIndex;

        // Unique id which is provided by the client in order to avoid re-applying the command if it sent to different nodes or on retry.
        // if string.Empty passed, it will be treated as don't care,
        // if (null) value is passed, it will be treated as a bug. (will throw an exception only in Debug builds to support old clients)
        public string UniqueRequestId;

        protected CommandBase() { }

        protected CommandBase(string uniqueRequestId)
        {
            UniqueRequestId = uniqueRequestId;
        }

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

        public virtual string AdditionalDebugInformation(Exception exception)
        {
            return null;
        }
    }
}
