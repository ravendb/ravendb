using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class DeleteMultipleValuesCommand : CommandBase
    {
        public List<string> Names;

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var djv = base.ToJson(context);
            djv[nameof(Names)] = Names;

            return djv;
        }

        public override void VerifyCanExecuteCommand(ServerStore store, TransactionOperationContext context, bool isClusterAdmin)
        {
            if (Names.Contains(ServerStore.LicenseStorageKey) || Names.Any(name => name.StartsWith(Constants.Certificates.Prefix)))
                throw new RachisApplyException("Attempted to use DeleteMultipleValuesCommand to delete certificates or a license, use dedicated commands for this.");
        }
    }
}
