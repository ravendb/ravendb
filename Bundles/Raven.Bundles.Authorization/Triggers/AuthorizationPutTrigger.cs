using System;
using System.IO;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Plugins;

namespace Raven.Bundles.Authorization.Triggers
{
	public class AuthorizationPutTrigger : AbstractPutTrigger
	{
		/// <summary>
		/// Reset the cache for the newly put document if it is a raven authorization document
		/// </summary>
		public override void AfterPut(string key, JObject document, JObject metadata, Guid etag,
		                              TransactionInformation transactionInformation)
		{
			if (key.StartsWith("/Raven/Authorization", StringComparison.InvariantCultureIgnoreCase))
				AuthorizationDecisions.RemoveDocumentFromCache(key);
		}

		public override VetoResult AllowPut(string key, JObject document, JObject metadata, TransactionInformation transactionInformation)
		{
			if(AuthorizationContext.IsInAuthorizationContext)
				return VetoResult.Allowed;

			var user = CurrentRavenOperation.Headers.Value[Constants.RavenAuthorizationUser];
			var operation = CurrentRavenOperation.Headers.Value[Constants.RavenAuthorizationOperation];
			if(string.IsNullOrEmpty(operation) || string.IsNullOrEmpty(user))
				return VetoResult.Allowed;

			var previousDocument = Database.Get(key, transactionInformation);
			var metadataForAuthorization = previousDocument != null ? previousDocument.Metadata : metadata;

			var authorizationDecisions = AuthorizationDecisions.GetOrCreateSingleton(Database);
			var sw = new StringWriter();
			var isAllowed = authorizationDecisions.IsAllowed(user, operation, key, metadataForAuthorization, sw.WriteLine);
			return isAllowed ? 
				VetoResult.Allowed : 
				VetoResult.Deny(sw.GetStringBuilder().ToString());
		}
	}
}