using Raven.Server.ServerWide.Context;

namespace Raven.Server.ServerWide.Commands
{
    public class DeactivateLicenseCommand : DeleteValueCommand
    {
        public override void VerifyCanExecuteCommand(ServerStore store, TransactionOperationContext context, bool isClusterAdmin)
        {
            AssertClusterAdmin(isClusterAdmin);
        }
    }
}
