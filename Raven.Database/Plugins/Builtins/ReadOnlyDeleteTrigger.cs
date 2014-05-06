// -----------------------------------------------------------------------
//  <copyright file="ReadOnlyDeleteTrigger.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;

namespace Raven.Database.Plugins.Builtins
{
    public class ReadOnlyDeleteTrigger : AbstractDeleteTrigger
    {
        public override VetoResult AllowDelete(string key, TransactionInformation transactionInformation)
        {
            var old = Database.Documents.Get(key, transactionInformation);
            if (old == null)
                return VetoResult.Allowed;

            var isOldReadOnly = old.Metadata.Value<bool>(Constants.RavenReadOnly);

            if (isOldReadOnly)
                return VetoResult.Deny(string.Format("You cannot delete document '{0}' because it is marked as readonly. Consider changing '{1}' flag to 'False'.", key, Constants.RavenReadOnly));

            return VetoResult.Allowed;
        }
    }
}