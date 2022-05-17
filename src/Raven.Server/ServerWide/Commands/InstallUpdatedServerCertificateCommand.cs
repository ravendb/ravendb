using Raven.Client.Util;
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

        public InstallUpdatedServerCertificateCommand(string certificate, bool replaceImmediately, string uniqueRequestId) : base(uniqueRequestId)
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

        public ConfirmReceiptServerCertificateCommand(string thumbprint) : base(RaftIdGenerator.DontCareId)
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

        public RecheckStatusOfServerCertificateCommand() : base(RaftIdGenerator.DontCareId)
        {
        }
    }

    public class ConfirmServerCertificateReplacedCommand : CommandBase
    {
        public string Thumbprint { get; set; }
        public string OldThumbprint { get; set; }

        public ConfirmServerCertificateReplacedCommand()
        {
            // for deserialization
        }

        public ConfirmServerCertificateReplacedCommand(string thumbprint, string oldThumbprint) : base(RaftIdGenerator.DontCareId)
        {
            Thumbprint = thumbprint;
            OldThumbprint = oldThumbprint;
        }

        public override void VerifyCanExecuteCommand(ServerStore store, TransactionOperationContext context, bool isClusterAdmin)
        {
            AssertClusterAdmin(isClusterAdmin);
        }

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var json = base.ToJson(context);
            json[nameof(Thumbprint)] = Thumbprint;
            json[nameof(OldThumbprint)] = OldThumbprint;
            return json;
        }
    }

    public class RecheckStatusOfServerCertificateReplacementCommand : CommandBase
    {
        public override void VerifyCanExecuteCommand(ServerStore store, TransactionOperationContext context, bool isClusterAdmin)
        {
            AssertClusterAdmin(isClusterAdmin);
        }

        public RecheckStatusOfServerCertificateReplacementCommand() : base(RaftIdGenerator.DontCareId)
        {
        }
    }
}
