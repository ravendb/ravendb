using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util.Encryptors;
using Raven.Bundles.Authorization.Model;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.WebApi.Attributes;

namespace Raven.Bundles.Authorization.Controllers
{
    [InheritedExport(typeof(BaseDatabaseApiController))]
    [ExportMetadata("Bundle", "Authorization")]
    [ExportMetadata("IsRavenExternalBundle", true)]
    [ExportMetadata("IsRavenExternalBundleController",true)]
    public class IsAllowedController : BaseDatabaseApiController
    {
        [HttpGet]
        [RavenRoute("authorization/IsAllowed/{*userId}")]
        [RavenRoute("databases/{databaseName}/authorization/IsAllowed/{*userId}")]
        public HttpResponseMessage IsAllowed(string userId)
        {
            //var userId = id;

            var docIds = GetQueryStringValues("id");
            var operation = GetQueryStringValue("operation");
            var transactionInformation = GetRequestTransaction();

            if (docIds == null || string.IsNullOrEmpty(operation) || string.IsNullOrEmpty(userId))
            {
                return GetEmptyMessage(HttpStatusCode.BadRequest);
            }

            // we don't want security to take hold when we are trying to ask about security
            using (Database.DisableAllTriggersForCurrentThread())
            {
                var documents = docIds.Select(docId => Database.Documents.GetDocumentMetadata(docId, transactionInformation)).ToArray();
                var etag = CalculateEtag(documents, userId);
                if (MatchEtag(etag))
                {
                    return GetEmptyMessage(HttpStatusCode.NotModified);
                }
                var msg = GetMessageWithObject(GenerateAuthorizationResponse(documents, docIds, operation, userId));
                WriteETag(etag, msg);
                return msg;
            }
        }

        private List<OperationAllowedResult> GenerateAuthorizationResponse(JsonDocumentMetadata[] documents, string[] docIds, string operation, string userId)
        {
            var list = new List<OperationAllowedResult>();
            for (var index = 0; index < documents.Length; index++)
            {
                var document = documents[index];
                var docId = docIds[index];
                if (document == null)
                {
                    list.Add(new OperationAllowedResult
                    {
                        IsAllowed = false,
                        Reasons = new List<string>
                        {
                            "Document " + docId + " does not exists"
                        }
                    });
                    continue;
                }
                var reasons = new List<string>();
                var authorizationDecisions = new AuthorizationDecisions(Database);
                var isAllowed = authorizationDecisions.IsAllowed(userId, operation, docId, document.Metadata, reasons.Add);

                list.Add(new OperationAllowedResult { IsAllowed = isAllowed, Reasons = reasons });
            }
            return list;
        }

        private Etag CalculateEtag(IEnumerable<JsonDocumentMetadata> documents, string userId)
        {
                var etags = new List<byte>();

                etags.AddRange(documents.SelectMany(x => x == null ? Guid.Empty.ToByteArray() : (x.Etag ?? Etag.Empty).ToByteArray()));

                var userDoc = Database.Documents.Get(userId, null);
                if (userDoc == null)
                {
                    etags.AddRange(Guid.Empty.ToByteArray());
                }
                else
                {
                    etags.AddRange((userDoc.Etag ?? Etag.Empty).ToByteArray());
                    var user = userDoc.DataAsJson.JsonDeserialization<AuthorizationUser>();
                    foreach (var roleMetadata in user.Roles.Select(role => Database.Documents.GetDocumentMetadata(role, null)))
                    {
                        etags.AddRange(roleMetadata == null ? Guid.Empty.ToByteArray() : (roleMetadata.Etag ?? Etag.Empty).ToByteArray());
                    }
                }

            return Etag.Parse(Encryptor.Current.Hash.Compute16(etags.ToArray()));
            }
    }
}
