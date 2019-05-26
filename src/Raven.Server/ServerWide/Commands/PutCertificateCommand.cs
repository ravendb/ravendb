using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class PutCertificateCommand : PutValueCommand<CertificateDefinition>
    {
        public string PublicKeyPinningHash;

        public PutCertificateCommand()
        {
            // for deserialization
        }

        public PutCertificateCommand(string name, CertificateDefinition value, string uniqueRequestId) : base(uniqueRequestId)
        {
            Name = name;
            Value = value;
            PublicKeyPinningHash = value.PublicKeyPinningHash;
        }

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var djv = base.ToJson(context);
            djv[nameof(Name)] = Name;
            djv[nameof(Value)] = ValueToJson();
            djv[nameof(PublicKeyPinningHash)] = PublicKeyPinningHash;
            return djv;
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
