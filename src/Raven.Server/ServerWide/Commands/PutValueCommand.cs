using System;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public abstract class PutValueCommand<T> : CommandBase
    {
        public string Name;

        public T Value;

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var djv = base.ToJson(context);
            djv[nameof(Name)] = Name;
            djv[nameof(Value)] = ValueToJson();

            return djv;
        }

        public abstract DynamicJsonValue ValueToJson();

        public virtual void UpdateValue(ClusterOperationContext context, long index)
        {
        }

        public override void VerifyCanExecuteCommand(ServerStore store, TransactionOperationContext context, bool isClusterAdmin)
        {
            if (Name == ServerStore.LicenseStorageKey ||
                Name == ServerStore.LicenseLimitsStorageKey)
                throw new InvalidOperationException($"Attempted to use {nameof(PutValueCommand<T>)} to delete a license, use dedicated command for this.");
        }

        protected PutValueCommand(string uniqueRequestId) : base(uniqueRequestId)
        {
        }

        protected PutValueCommand()
        {
        }
    }
}
