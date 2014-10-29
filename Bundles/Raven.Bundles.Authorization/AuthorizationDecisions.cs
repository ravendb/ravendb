//-----------------------------------------------------------------------
// <copyright file="AuthorizationDecisions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Text;
using Raven.Abstractions.Extensions;
using Raven.Bundles.Authorization.Model;
using Raven.Database;
using System.Linq;
using Raven.Json.Linq;

namespace Raven.Bundles.Authorization
{
	public class AuthorizationDecisions
	{
		public const string RavenDocumentAuthorization = "Raven-Document-Authorization";

		private readonly DocumentDatabase database;

		public AuthorizationDecisions(DocumentDatabase database)
		{
			this.database = database;
		}

		public bool IsAllowed(
			string userId,
			string operation,
			string documentId,
			RavenJObject documentMetadata,
			Action<string> logger)
		{
			var authAsJson = documentMetadata[RavenDocumentAuthorization] as RavenJObject;
			if (authAsJson == null)
			{
				if (logger != null)
					logger("Document " + documentId + " is not secured and can be accessed by everyone.");
				return true;
			}
			var documentAuthorization = authAsJson.JsonDeserialization<DocumentAuthorization>();
			var user = GetDocumentAsEntity<AuthorizationUser>(userId);
			if (user == null)
			{
				if (logger != null)
					logger("Could not find user: " + userId + " for secured document: " + documentId);
				return false;
			}
			IEnumerable<IPermission> permissions =
				from permission in documentAuthorization.Permissions // permissions for user / role directly on document
				where DocumentPermissionMatchesUser(permission, user, userId)
				where OperationMatches(permission.Operation, operation)
				select permission;

			permissions = permissions.Concat( // permissions on user matching the document's tags
				from permission in user.Permissions
				where OperationMatches(permission.Operation, operation)
				where TagsMatch(permission.Tags, documentAuthorization.Tags)
				select permission
				);

			permissions = permissions.Concat( // permissions on all user's roles with tags matching the document
				from roleName in GetHierarchicalNames(user.Roles)
				let role = GetDocumentAsEntity<AuthorizationRole>(roleName)
				where role != null
				from permission in role.Permissions
				where OperationMatches(permission.Operation, operation)
				where TagsMatch(permission.Tags, documentAuthorization.Tags)
				select permission
				);

			IEnumerable<IPermission> orderedPermissions = permissions.OrderByDescending(x => x.Priority).ThenBy(x => x.Allow);
			if (logger != null)
			{
				var list = orderedPermissions.ToList(); // avoid iterating twice on the list
				orderedPermissions = list;
				foreach (var permission in list)
				{
					logger(permission.Explain);
				}
			}
			var decidingPermission = orderedPermissions
				.FirstOrDefault();

			if (decidingPermission == null)
			{
				if (logger != null)
				{
					ExplainWhyUserCantAccessTheDocument(logger, documentId, userId, user, documentAuthorization, operation);
				}
				return false;
			}

			return decidingPermission.Allow;
		}

		private static void ExplainWhyUserCantAccessTheDocument(Action<string> logger, string documentId, string userId, AuthorizationUser user, DocumentAuthorization documentAuthorization, string operation)
		{
			var sb = new StringBuilder("Could not find any permissions for operation: ")
				.Append(operation)
				.Append(" on ")
				.Append(documentId)
				.Append(" for user ")
				.Append(userId)
				.Append(".");

			if(user.Roles.Count > 0)
			{
				sb.Append(" or the user's roles: [")
					.Append(string.Join(", ", user.Roles))
					.Append("]");
			}
			sb.AppendLine();

			if(documentAuthorization.Permissions.Count(x=>x.Operation.Equals(operation, StringComparison.InvariantCultureIgnoreCase)) == 0)
			{
				sb.Append("No one may perform operation ")
					.Append(operation)
					.Append(" on ")
					.Append(documentId);
			}
			else
			{
				sb.Append("Only the following may perform operation ")
					.Append(operation)
					.Append(" on ")
					.Append(documentId)
					.AppendLine(":");

				foreach (var documentPermission in documentAuthorization.Permissions)
				{
					sb.Append("\t")
						.Append(documentPermission.Explain)
						.AppendLine();
				}
			}

			logger(sb.ToString());
		}

		private static bool DocumentPermissionMatchesUser(DocumentPermission permission, AuthorizationUser user, string userId)
		{
			if (permission.User != null)
				return string.Equals(permission.User, userId, StringComparison.InvariantCultureIgnoreCase);
			if (permission.Role == null)
				return false;

			return GetHierarchicalNames(user.Roles).Any(role => permission.Role.Equals(role, StringComparison.InvariantCultureIgnoreCase));
		}

		private static string GetParentName(string operationName)
		{
			int lastIndex = operationName.LastIndexOf('/');
			if (lastIndex == -1)
				return "";
			return operationName.Substring(0, lastIndex);
		}

		private static IEnumerable<string> GetHierarchicalNames(IEnumerable<string> names)
		{
			var hierarchicalNames = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase);
			foreach (var name in names)
			{
				var copy = name;
				do
				{
					hierarchicalNames.Add(copy);
					copy = GetParentName(copy);
				} while (copy != "");
			}
			return hierarchicalNames;
		}

		private static bool OperationMatches(string op1, string op2)
		{
			return op2.StartsWith(op1, StringComparison.InvariantCultureIgnoreCase);
		}

		private static bool TagsMatch(IEnumerable<string> permissionTags, IEnumerable<string> documentTags)
		{
			return permissionTags.All(p => documentTags.Any(d => d.StartsWith(p, StringComparison.InvariantCultureIgnoreCase)));
		}

		private T GetDocumentAsEntity<T>(string documentId) where T : class
		{
			var document = database.Documents.Get(documentId, null);
			if (document == null)
				return null;
			var entity = document.DataAsJson.JsonDeserialization<T>();
			return entity;
		}
	}
}
