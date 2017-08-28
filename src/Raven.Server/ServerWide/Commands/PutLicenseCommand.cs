using Raven.Server.Commercial;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class PutLicenseCommand : PutValueCommand<License>
    {
        public PutLicenseCommand()
        {
            // for deserialization
        }

        public PutLicenseCommand(string name, License license)
        {
            Name = name;
            Value = license;
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
