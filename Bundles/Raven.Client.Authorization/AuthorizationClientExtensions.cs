using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Bundles.Authorization;
using Raven.Bundles.Authorization.Model;

namespace Raven.Client.Authorization
{
	public static class AuthorizationClientExtensions
	{
		public const string RavenDocumentAuthorization = "Raven-Document-Authorization";
		
		public static DocumentAuthorization GetAuthorizationFor(this IDocumentSession session, object entity)
		{
			var metadata = session.GetMetadataFor(entity);
			var docAuthAsJson = metadata[RavenDocumentAuthorization];
			if(docAuthAsJson == null)
				return null;
			return new JsonSerializer
			{
				ContractResolver = session.Conventions.JsonContractResolver,
			}.Deserialize<DocumentAuthorization>(new JTokenReader(docAuthAsJson));
		}

		public static void SetAuthorizationFor(this IDocumentSession session, object entity, DocumentAuthorization documentAuthorization)
		{
			var metadata = session.GetMetadataFor(entity);
			metadata[RavenDocumentAuthorization] = JObject.FromObject(documentAuthorization, new JsonSerializer
			{
				ContractResolver = session.Conventions.JsonContractResolver,
			});
		}

		public static void SecureFor(this IDocumentSession session, string userId, string operation)
		{
			session.DatabaseCommands.OperationsHeaders[Constants.RavenAuthorizationUser] = userId;
			session.DatabaseCommands.OperationsHeaders[Constants.RavenAuthorizationOperation] = operation;
		}
	}
}