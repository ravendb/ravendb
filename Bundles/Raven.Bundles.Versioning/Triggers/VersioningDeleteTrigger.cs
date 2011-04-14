//-----------------------------------------------------------------------
// <copyright file="VersioningDeleteTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Database;
using Raven.Database.Plugins;
using Raven.Http;

namespace Raven.Bundles.Versioning.Triggers
{
    public class VersioningDeleteTrigger : AbstractDeleteTrigger
    {
        public override VetoResult AllowDelete(string key, TransactionInformation transactionInformation)
        {
            JsonDocument document = Database.Get(key, transactionInformation);
            if (document == null)
                return VetoResult.Allowed;
            if (document.Metadata.Value<string>(VersioningPutTrigger.RavenDocumentRevisionStatus) != "Historical")
                return VetoResult.Allowed;
            return VetoResult.Deny("Deleting a historical revision is not allowed");
        }
    }
}
