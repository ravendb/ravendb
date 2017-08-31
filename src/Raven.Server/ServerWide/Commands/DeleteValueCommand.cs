using System;
using Raven.Client;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class DeleteValueCommand : CommandBase
    {
        public string Name;

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var djv = base.ToJson(context);
            djv[nameof(Name)] = Name;

            return djv;
        }

        public override void VerifyCanExecuteCommand(ServerStore store, TransactionOperationContext context, bool isClusterAdmin)
        {
            if (Name == ServerStore.LicenseStorageKey || Name.StartsWith(Constants.Certificates.Prefix))
                throw new InvalidOperationException("Attempted to use DeleteValueCommand to delete a certificate or license, use dedicated commands for this.");
        }
    }
}
