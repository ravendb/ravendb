using System.IO;
using System.Web;
using Raven.Database;
using Raven.Database.Plugins;

namespace Raven.Bundles.Authorization.Triggers
{
	public class AuthorizationDeleteTrigger : AbstractDeleteTrigger
	{
		public AuthorizationDecisions AuthorizationDecisions { get; set; }

		public override void Initialize()
		{
			AuthorizationDecisions = new AuthorizationDecisions(Database, HttpRuntime.Cache);
		}

		public override VetoResult AllowDelete(string key, TransactionInformation transactionInformation)
		{
			if (AuthorizationContext.IsInAuthorizationContext)
				return VetoResult.Allowed;

			using(AuthorizationContext.Enter())
			{
				var user = CurrentRavenOperation.Headers.Value[Constants.RavenAuthorizationUser];
				var operation = CurrentRavenOperation.Headers.Value[Constants.RavenAuthorizationOperation];
				if (string.IsNullOrEmpty(operation) || string.IsNullOrEmpty(user))
					return VetoResult.Allowed;

				var previousDocument = Database.Get(key, transactionInformation);
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
