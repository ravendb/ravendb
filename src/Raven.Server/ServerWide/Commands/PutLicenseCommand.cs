using Raven.Server.Commercial;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public sealed class PutLicenseCommand : PutValueCommand<License>
    {
        public bool SkipLicenseAssertion;

        public PutLicenseCommand()
        {
            // for deserialization
        }

        public PutLicenseCommand(string name, License license, string uniqueRequestId, bool fromApi = false) : base(uniqueRequestId)
        {
            Name = name;
            Value = license;
            SkipLicenseAssertion = fromApi;
        }

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var djv = base.ToJson(context);
            djv[nameof(SkipLicenseAssertion)] = SkipLicenseAssertion;

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
