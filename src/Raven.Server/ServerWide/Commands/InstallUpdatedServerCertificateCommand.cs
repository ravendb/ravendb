using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class InstallUpdatedServerCertificateCommand : CommandBase
    {
        public string Certificate { get; set; }
        public bool ReplaceImmediately { get; set; }

        public InstallUpdatedServerCertificateCommand()
        {
            // for deserialization
        }

        public InstallUpdatedServerCertificateCommand(string certificate, bool replaceImmediately)
        {
            Certificate = certificate;
            ReplaceImmediately = replaceImmediately;
        }

        public override void VerifyCanExecuteCommand(ServerStore store, TransactionOperationContext context, bool isClusterAdmin)
        {
            AssertClusterAdmin(isClusterAdmin);
        }

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var json = base.ToJson(context);
            json[nameof(Certificate)] = Certificate;
            json[nameof(ReplaceImmediately)] = ReplaceImmediately;
            return json;
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

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var json = base.ToJson(context);
            json[nameof(Thumbprint)] = Thumbprint;
            return json;
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
