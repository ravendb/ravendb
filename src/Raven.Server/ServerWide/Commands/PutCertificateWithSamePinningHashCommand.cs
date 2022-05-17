using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class PutCertificateWithSamePinningHashCommand : PutCertificateCommand
    {
        public PutCertificateWithSamePinningHashCommand()
        {
            // for deserialization
        }

        public PutCertificateWithSamePinningHashCommand(string name, CertificateDefinition value, string uniqueRequestId) : base(name, value, uniqueRequestId)
        {

        }

        public override DynamicJsonValue ValueToJson()
        {
            return Value?.ToJson();
        }

        public override void VerifyCanExecuteCommand(ServerStore store, TransactionOperationContext context, bool isClusterAdmin)
        {
            AssertClusterAdmin(isClusterAdmin);
        }
    }
}
