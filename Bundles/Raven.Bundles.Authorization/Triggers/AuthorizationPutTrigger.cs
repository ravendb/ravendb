//-----------------------------------------------------------------------
// <copyright file="AuthorizationPutTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.IO;
using Raven.Abstractions.Data;
using Raven.Database.Plugins;
using Raven.Database.Server;
using Raven.Json.Linq;

namespace Raven.Bundles.Authorization.Triggers
{
    public class AuthorizationPutTrigger : AbstractPutTrigger
    {
        public AuthorizationDecisions AuthorizationDecisions { get; set; }

        public override void Initialize()
        {
            AuthorizationDecisions = new AuthorizationDecisions(Database);
        }

        public override VetoResult AllowPut(string key, RavenJObject document, RavenJObject metadata)
        {
            using (Database.DisableAllTriggersForCurrentThread())
            {
                var user = (CurrentOperationContext.Headers.Value == null) ? null : CurrentOperationContext.Headers.Value.Value[Constants.Authorization.RavenAuthorizationUser];
                var operation = (CurrentOperationContext.Headers.Value == null) ? null : CurrentOperationContext.Headers.Value.Value[Constants.Authorization.RavenAuthorizationOperation];
                if (string.IsNullOrEmpty(operation) || string.IsNullOrEmpty(user))
                    return VetoResult.Allowed;

                var previousDocument = Database.Documents.Get(key);
                var metadataForAuthorization = previousDocument != null ? previousDocument.Metadata : metadata;

                var sw = new StringWriter();
                var isAllowed = AuthorizationDecisions.IsAllowed(user, operation, key, metadataForAuthorization, sw.WriteLine);
                return isAllowed ?
                    VetoResult.Allowed :
                    VetoResult.Deny(sw.GetStringBuilder().ToString());
            }
        }
    }
}
