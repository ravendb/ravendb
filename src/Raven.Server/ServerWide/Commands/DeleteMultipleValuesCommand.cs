using System.Collections.Generic;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class DeleteMultipleValuesCommand : CommandBase
    {
        public List<string> Names;

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var djv = base.ToJson(context);
            djv[nameof(Names)] = Names;

            return djv;
        }

        public override void VerifyCanExecuteCommand(ServerStore store, TransactionOperationContext context, bool isClusterAdmin)
        {
            if (Names.Contains(ServerStore.LicenseStorageKey))
                throw new RachisApplyException($"Attempted to use {nameof(DeleteMultipleValuesCommand)} to delete a license, use dedicated command for this.");
        }

        public DeleteMultipleValuesCommand()
        {
        }

        public DeleteMultipleValuesCommand(string uniqueRequestId) : base(uniqueRequestId)
        {
        }

        public virtual void AfterDelete(ServerStore store, ClusterOperationContext context)
        {
        }
    }
}
