// -----------------------------------------------------------------------
//  <copyright file="ReadOnlyPutTrigger.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Database.Plugins.Builtins
{
    public class ReadOnlyPutTrigger : AbstractPutTrigger
    {
        public override VetoResult AllowPut(string key, RavenJObject document, RavenJObject metadata, TransactionInformation transactionInformation)
        {
            var isNewReadOnly = metadata.Value<bool>(Constants.RavenReadOnly);
            if (!isNewReadOnly)
                return VetoResult.Allowed;

            var old = Database.Documents.Get(key, transactionInformation);
            if (old == null)
                return VetoResult.Allowed;

            var isOldReadOnly = old.Metadata.Value<bool>(Constants.RavenReadOnly);
            
            if (isOldReadOnly)
                return VetoResult.Deny($"You cannot update document '{key}' when both of them, new and existing one, are marked as readonly. To update this document change '{Constants.RavenReadOnly}' flag to 'False' or remove it entirely.");

            return VetoResult.Allowed;
        }
    }
}