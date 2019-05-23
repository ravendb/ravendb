using Raven.Server.ServerWide.Context;

namespace Raven.Server.ServerWide.Commands
{
    public class DeleteServerWideBackupConfigurationCommand : DeleteValueCommand
    {
        public DeleteServerWideBackupConfigurationCommand()
        {
            Name = ClusterStateMachine.BackupTemplateConfigurationName;
        }

        public override void VerifyCanExecuteCommand(ServerStore store, TransactionOperationContext context, bool isClusterAdmin)
        {
            AssertClusterAdmin(isClusterAdmin);
        }
    }
}
