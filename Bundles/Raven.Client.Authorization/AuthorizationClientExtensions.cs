//-----------------------------------------------------------------------
// <copyright file="AuthorizationClientExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Text;
using Raven.Abstractions.Extensions;
using Raven.Client.Document;
using Raven.Imports.Newtonsoft.Json;
using Raven.Bundles.Authorization;
using System.Linq;
using Raven.Bundles.Authorization.Model;
using Raven.Client.Connection;
using Raven.Json.Linq;

namespace Raven.Client.Authorization
{
	public static class AuthorizationClientExtensions
	{
		public const string RavenDocumentAuthorization = "Raven-Document-Authorization";

		public static OperationAllowedResult IsOperationAllowedOnDocument(this ISyncAdvancedSessionOperation session, string userId, string operation, string documentId)
		{
			return IsOperationAllowedOnDocument(session, userId, operation, new[] {documentId}).First();
		}

		public static OperationAllowedResult[] IsOperationAllowedOnDocument(this ISyncAdvancedSessionOperation session, string userId, string operation, params string[] documentIds)
		{
			var serverClient = ((DocumentSession)session).DatabaseCommands as ServerClient;
			if (serverClient == null)
				throw new InvalidOperationException("Cannot get whatever operation is allowed on document in embedded mode.");

			var url = new StringBuilder("/authorization/IsAllowed/")
				.Append(Uri.EscapeUriString(userId))
				.Append("?operation=")
				.Append(Uri.EscapeUriString(operation));

			foreach (var docId in documentIds)
			{
				url.Append("&id=").Append(Uri.EscapeUriString(docId));
			}

			var result = serverClient.ExecuteGetRequest(url.ToString());

			return
				session.DocumentStore.Conventions.CreateSerializer().Deserialize<OperationAllowedResult[]>(
					new RavenJTokenReader(result));
		}

		public static DocumentAuthorization GetAuthorizationFor(this IDocumentSession session, object entity)
		{
			var metadata = session.Advanced.GetMetadataFor(entity);
			var docAuthAsJson = metadata[RavenDocumentAuthorization];
			if (docAuthAsJson == null)
				return null;
			var jsonSerializer = JsonExtensions.CreateDefaultJsonSerializer();
			jsonSerializer.ContractResolver = session.Advanced.DocumentStore.Conventions.JsonContractResolver;
			return jsonSerializer.Deserialize<DocumentAuthorization>(new RavenJTokenReader(docAuthAsJson));
		}

		public static void SetAuthorizationFor(this IDocumentSession session, object entity, DocumentAuthorization documentAuthorization)
		{
			var metadata = session.Advanced.GetMetadataFor(entity);
			var jsonSerializer = JsonExtensions.CreateDefaultJsonSerializer();
			jsonSerializer.ContractResolver = session.Advanced.DocumentStore.Conventions.JsonContractResolver;
			metadata[RavenDocumentAuthorization] = RavenJObject.FromObject(documentAuthorization, jsonSerializer);
		}

		public static bool IsAllowed(
			this IDocumentSession session,
			AuthorizationUser user,
			string operation)
		{
			if (session == null) throw new ArgumentNullException("session");
			if (user == null) throw new ArgumentNullException("user");
			if (operation == null) throw new ArgumentNullException("operation");

			IEnumerable<IPermission> permissions =
				from permission in user.Permissions ?? new List<OperationPermission>()// permissions for user / role directly on document
				where OperationMatches(permission.Operation, operation)
				select permission;

			session.Load<AuthorizationRole>(user.Roles.Where(roleId => session.Advanced.IsLoaded(roleId) == false));

			permissions = permissions.Concat(
				from roleId in user.Roles
				let role = session.Load<AuthorizationRole>(roleId)
				where role != null
				from permission in role.Permissions ?? new List<OperationPermission>()
				where OperationMatches(permission.Operation, operation)
				select permission
				);

			IEnumerable<IPermission> orderedPermissions = permissions.OrderByDescending(x => x.Priority).ThenBy(x => x.Allow);

			var decidingPermission = orderedPermissions.FirstOrDefault();

			return decidingPermission != null && decidingPermission.Allow;
		}


		private static bool OperationMatches(string op1, string op2)
		{
			return op2.StartsWith(op1);
		}

		public static void SecureFor(this IDocumentSession session, string userId, string operation)
		{
			var databaseCommands = ((DocumentSession)session).DatabaseCommands;
			databaseCommands.OperationsHeaders[Constants.RavenAuthorizationUser] = userId;
			databaseCommands.OperationsHeaders[Constants.RavenAuthorizationOperation] = operation;
		}
	}
}
