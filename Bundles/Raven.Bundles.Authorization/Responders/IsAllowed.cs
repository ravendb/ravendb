using System;
using System.Collections.Generic;
using Raven.Database.Server.Responders;
using Raven.Http.Abstractions;
using Raven.Http.Extensions;

namespace Raven.Bundles.Authorization.Responders
{
	public class IsAllowed : RequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/authorization/IsAllowed/(.+)"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new string[]{"GET"}; }
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

			var list = new List<OperationAllowedResult>();
			// we don't want security to take hold when we are trying to ask about security
			using (Database.DisableAllTriggersForCurrentThread()) 
			{
				foreach (var docId in docIds)
				{
					var document = Database.GetDocumentMetadata(docId, transactionInformation);

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

					list.Add(new OperationAllowedResult{ IsAllowed = isAllowed, Reasons = reasons });
				}
			}
			context.WriteJson(list);
		}
	}
}