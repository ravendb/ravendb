using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class PutCertificateCommand : PutValueCommand<CertificateDefinition>
    {
        public PutCertificateCommand()
        {
            // for deserialization
        }

        public PutCertificateCommand(string name, CertificateDefinition value)
        {
            Name = name;
            Value = value;
        }

        public override DynamicJsonValue ValueToJson()
        {
            return Value?.ToJson();
        }

        public override void VerifyCanExecuteCommand(ServerStore store, TransactionOperationContext context, bool isClusterAdmin)
        {
            if (Value.SecurityClearance == SecurityClearance.ClusterAdmin || Value.SecurityClearance != SecurityClearance.ClusterNode)
                AssertClusterAdmin(isClusterAdmin);
        }
    }
}
