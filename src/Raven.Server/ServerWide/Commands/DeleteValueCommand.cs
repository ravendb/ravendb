using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class DeleteValueCommand : CommandBase
    {
        public string Name;

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var djv = base.ToJson(context);
            djv[nameof(Name)] = Name;

            return djv;
        }

        public virtual void DeleteValue(ClusterOperationContext context)
        {
        }

        public override void VerifyCanExecuteCommand(ServerStore store, TransactionOperationContext context, bool isClusterAdmin)
        {
            if (Name == ServerStore.LicenseStorageKey)
                throw new RachisApplyException($"Attempted to use {nameof(DeleteValueCommand)} to delete a license, use dedicated command for this.");
        }

        public DeleteValueCommand()
        {
        }

        public DeleteValueCommand(string uniqueRequestId) : base(uniqueRequestId)
        {
        }
    }
}
