using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using Raven.Abstractions.Data;
using Raven.Bundles.Authorization.Model;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Responders;
using System.Linq;
using Raven.Abstractions.Extensions;

namespace Raven.Bundles.Authorization.Responders
{
	public class IsAllowed : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/authorization/IsAllowed/(.+)"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new string[] { "GET" }; }
		}

		public override void Respond(IHttpContext context)
		{
			var match = urlMatcher.Match(context.GetRequestUrl());
			var userId = match.Groups[1].Value;

			var docIds = context.Request.QueryString.GetValues("id");
			var operation = context.Request.QueryString["operation"];
			var transactionInformation = GetRequestTransaction(context);

			if (docIds == null || string.IsNullOrEmpty(operation) || string.IsNullOrEmpty(userId))
			{
				context.SetStatusToBadRequest();
				return;
			}


			// we don't want security to take hold when we are trying to ask about security
			using (Database.DisableAllTriggersForCurrentThread())
			{
				var documents = docIds.Select(docId=>Database.GetDocumentMetadata(docId, transactionInformation)).ToArray();
				var etag = CalculateEtag(documents, userId);
				if (context.MatchEtag(etag))
				{
					context.SetStatusToNotModified();
					return;
				}

				context.Response.AddHeader("ETag", etag.ToString());
				context.WriteJson(GenerateAuthorizationResponse(documents, docIds, operation, userId));
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

				list.Add(new OperationAllowedResult {IsAllowed = isAllowed, Reasons = reasons});
			}
			return list;
		}

		private Guid CalculateEtag(IEnumerable<JsonDocumentMetadata> documents, string userId)
		{
			Guid etag;

			using(var md5 = MD5.Create())
			{
				var etags =new List<byte>();

				etags.AddRange(documents.SelectMany(x => x == null ? Guid.Empty.ToByteArray() : (x.Etag ?? Guid.Empty).ToByteArray()));

				var userDoc = Database.Get(userId, null);
				if(userDoc == null)
				{
					etags.AddRange(Guid.Empty.ToByteArray());
				}
				else
				{
					etags.AddRange((userDoc.Etag ?? Guid.Empty).ToByteArray());
					var user = userDoc.DataAsJson.JsonDeserialization<AuthorizationUser>();
					foreach (var roleMetadata in user.Roles.Select(role => Database.GetDocumentMetadata(role, null)))
					{
						etags.AddRange(roleMetadata == null ? Guid.Empty.ToByteArray() : (roleMetadata.Etag ?? Guid.Empty).ToByteArray());
					}
				}

				etag = new Guid(md5.ComputeHash(etags.ToArray()));
			}
			return etag;
		}
	}
}