using System.IO;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Plugins;

namespace Raven.Bundles.Authorization.Triggers
{
	public class AuthorizationReadTrigger : AbstractReadTrigger
	{
		public override ReadVetoResult AllowRead(string key, JObject document, JObject metadata, ReadOperation readOperation,
		                                         TransactionInformation transactionInformation)
		{
			if (AuthorizationContext.IsInAuthorizationContext)
				return ReadVetoResult.Allowed;

			var user = CurrentRavenOperation.Headers.Value[Constants.RavenAuthorizationUser];
			var operation = CurrentRavenOperation.Headers.Value[Constants.RavenAuthorizationOperation];
			if (string.IsNullOrEmpty(operation) || string.IsNullOrEmpty(user))
				return ReadVetoResult.Allowed;

			var authorizationDecisions = AuthorizationDecisions.GetOrCreateSingleton(Database);
			var sw = new StringWriter();
			var isAllowed = authorizationDecisions.IsAllowed(user, operation, key, metadata, sw.WriteLine);
			if (isAllowed)
				return ReadVetoResult.Allowed;
			return readOperation == ReadOperation.Query ? 
				ReadVetoResult.Ignore : 
				ReadVetoResult.Deny(sw.GetStringBuilder().ToString());
		}
	}
}