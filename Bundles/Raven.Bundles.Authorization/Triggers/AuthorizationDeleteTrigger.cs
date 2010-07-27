using System.IO;
using Raven.Database;
using Raven.Database.Plugins;

namespace Raven.Bundles.Authorization.Triggers
{
	public class AuthorizationDeleteTrigger : AbstractDeleteTrigger
	{
		public override VetoResult AllowDelete(string key, Database.TransactionInformation transactionInformation)
		{
			if (AuthorizationContext.IsInAuthorizationContext)
				return VetoResult.Allowed;

			var user = CurrentRavenOperation.Headers.Value[Constants.RavenAuthorizationUser];
			var operation = CurrentRavenOperation.Headers.Value[Constants.RavenAuthorizationOperation];
			if (string.IsNullOrEmpty(operation) || string.IsNullOrEmpty(user))
				return VetoResult.Allowed;

			var previousDocument = Database.Get(key, transactionInformation);
			if(previousDocument == null)
				return VetoResult.Allowed;

			var authorizationDecisions = AuthorizationDecisions.GetOrCreateSingleton(Database);
			var sw = new StringWriter();
			var isAllowed = authorizationDecisions.IsAllowed(user, operation, key, previousDocument.Metadata, sw.WriteLine);
			return isAllowed ?
				VetoResult.Allowed :
				VetoResult.Deny(sw.GetStringBuilder().ToString());
		}
	}
}