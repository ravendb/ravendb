//-----------------------------------------------------------------------
// <copyright file="AuthorizationDeleteTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.ComponentModel.Composition;
using System.IO;
using Raven.Abstractions.Data;
using Raven.Database.Plugins;
using Raven.Database.Server;

namespace Raven.Bundles.Authorization.Triggers
{
    [InheritedExport(typeof(AbstractDeleteTrigger))]
    [ExportMetadata("Bundle", "Authorization")]
    [ExportMetadata("IsRavenExternalBundle",true)]
    public class AuthorizationDeleteTrigger : AbstractDeleteTrigger
    {
        public AuthorizationDecisions AuthorizationDecisions { get; set; }

        public override void Initialize()
        {
            AuthorizationDecisions = new AuthorizationDecisions(Database);
        }

        public override VetoResult AllowDelete(string key, TransactionInformation transactionInformation)
        {
            using(Database.DisableAllTriggersForCurrentThread())
            {
                var user = (CurrentOperationContext.Headers.Value == null) ? null : CurrentOperationContext.Headers.Value.Value[Constants.Authorization.RavenAuthorizationUser];
                var operation = (CurrentOperationContext.Headers.Value == null) ? null : CurrentOperationContext.Headers.Value.Value[Constants.Authorization.RavenAuthorizationOperation];
                if (string.IsNullOrEmpty(operation) || string.IsNullOrEmpty(user))
                    return VetoResult.Allowed;

                var previousDocument = Database.Documents.Get(key, transactionInformation);
                if (previousDocument == null)
                    return VetoResult.Allowed;

                var sw = new StringWriter();
                var isAllowed = AuthorizationDecisions.IsAllowed(user, operation, key, previousDocument.Metadata, sw.WriteLine);
                return isAllowed ?
                    VetoResult.Allowed :
                    VetoResult.Deny(sw.GetStringBuilder().ToString());
            }
        }
    }
}
