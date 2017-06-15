using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Bundles.Authorization;
using Raven.Bundles.Authorization.Model;
using Raven.Client.Connection.Async;
using Raven.Client.Document.Async;
using Raven.Json.Linq;

namespace Raven.Client.Authorization
{
    public static class RavenAuthorizationExtensions
    {
        public static async Task<OperationAllowedResult> IsOperationAllowedOnDocumentAsync(this IAsyncAdvancedSessionOperations session, string userId, string operation, string documentId)
        {
            return (await IsOperationAllowedOnDocumentAsync(session, userId, operation, new[] { documentId }).ConfigureAwait(false)).First();
        }

        public static async Task<OperationAllowedResult[]> IsOperationAllowedOnDocumentAsync(this IAsyncAdvancedSessionOperations session, string userId, string operation, params string[] documentIds)
        {
            var serverClient = ((AsyncDocumentSession)session).AsyncDatabaseCommands as AsyncServerClient;
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

            var result = await serverClient.ExecuteGetRequest(url.ToString()).ConfigureAwait(false);

            return
                session.DocumentStore.Conventions.CreateSerializer().Deserialize<OperationAllowedResult[]>(
                    new RavenJTokenReader(result));
        }

        public static DocumentAuthorization GetAuthorizationFor(this IAsyncDocumentSession session, object entity)
        {
            var metadata = session.Advanced.GetMetadataFor(entity);
            var docAuthAsJson = metadata[AuthorizationClientExtensions.RavenDocumentAuthorization];
            if (docAuthAsJson == null)
                return null;
            var jsonSerializer = JsonExtensions.CreateDefaultJsonSerializer();
            jsonSerializer.ContractResolver = session.Advanced.DocumentStore.Conventions.JsonContractResolver;
            return jsonSerializer.Deserialize<DocumentAuthorization>(new RavenJTokenReader(docAuthAsJson));
        }

        public static void SetAuthorizationFor(this IAsyncDocumentSession session, object entity, DocumentAuthorization documentAuthorization)
        {
            var metadata = session.Advanced.GetMetadataFor(entity);
            var jsonSerializer = JsonExtensions.CreateDefaultJsonSerializer();
            jsonSerializer.ContractResolver = session.Advanced.DocumentStore.Conventions.JsonContractResolver;
            metadata[AuthorizationClientExtensions.RavenDocumentAuthorization] = RavenJObject.FromObject(documentAuthorization, jsonSerializer);
        }

        public static async Task<bool> IsAllowedAsync(
            this IAsyncDocumentSession session,
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

            await session.LoadAsync<AuthorizationRole>(user.Roles.Where(roleId => session.Advanced.IsLoaded(roleId) == false)).ConfigureAwait(false);

            permissions = permissions.Concat(
                from roleId in user.Roles
                let role = session.LoadAsync<AuthorizationRole>(roleId).Result
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
            return op1 == op2 || op2.StartsWith(op1+"/");
        }

        public static void SecureFor(this IAsyncDocumentSession session, string userId, string operation)
        {
            var databaseCommands = ((AsyncDocumentSession)session).AsyncDatabaseCommands;
            databaseCommands.OperationsHeaders[Constants.Authorization.RavenAuthorizationUser] = userId;
            databaseCommands.OperationsHeaders[Constants.Authorization.RavenAuthorizationOperation] = operation;
        }
    }
}
