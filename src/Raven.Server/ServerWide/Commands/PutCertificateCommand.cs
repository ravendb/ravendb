using Raven.Client.ServerWide.Operations.Certificates;
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
    }
}