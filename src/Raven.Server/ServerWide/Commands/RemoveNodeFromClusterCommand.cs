using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class RemoveNodeFromClusterCommand : CommandBase
    {
        public string RemovedNode;

        public RemoveNodeFromClusterCommand()
        {
        }

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var json = base.ToJson(context);
            json[nameof(RemovedNode)] = RemovedNode;
            return json;
        }

        public override void VerifyCanExecuteCommand(ServerStore store, TransactionOperationContext context, bool isClusterAdmin)
        {
            AssertClusterAdmin(isClusterAdmin);
        }
    }
}
