using Raven.Server.ServerWide.Context;

namespace Raven.Server.ServerWide.Commands
{
    public class InstallUpdatedServerCertificateCommand : CommandBase
    {
        public string Certificate { get; set; }

        public InstallUpdatedServerCertificateCommand()
        {
            // for deserialization
        }

        public InstallUpdatedServerCertificateCommand(string certificate)
        {
            Certificate = certificate;
        }

        public override void VerifyCanExecuteCommand(ServerStore store, TransactionOperationContext context, bool isClusterAdmin)
        {
            AssertClusterAdmin(isClusterAdmin);
        }
    }
    
    public class ConfirmReceiptServerCertificateCommand : CommandBase
    {
        public string Thumbprint { get; set; }

        public ConfirmReceiptServerCertificateCommand()
        {
            // for deserialization
        }

        public ConfirmReceiptServerCertificateCommand(string thumbprint)
        {
            Thumbprint = thumbprint;
        }

        public override void VerifyCanExecuteCommand(ServerStore store, TransactionOperationContext context, bool isClusterAdmin)
        {
            AssertClusterAdmin(isClusterAdmin);
        }
    }
    
    public class RecheckStatusOfServerCertificateCommand : CommandBase
    {

        public override void VerifyCanExecuteCommand(ServerStore store, TransactionOperationContext context, bool isClusterAdmin)
        {
            AssertClusterAdmin(isClusterAdmin);
        }
    }
    
    
}
