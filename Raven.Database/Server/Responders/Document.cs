using System;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using Raven.Database.Json;
using System.Linq;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public class Document : RequestResponder
	{
		public override string UrlPattern
		{
			get { return @"/docs/(.+)"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] {"GET", "DELETE", "PUT", "PATCH"}; }
		}

		public override void Respond(IHttpContext context)
		{
			var match = urlMatcher.Match(context.GetRequestUrl());
			var docId = match.Groups[1].Value;
			switch (context.Request.HttpMethod)
			{
				case "GET":
					context.Response.Headers["Content-Type"] = "application/json; charset=utf-8";
					var doc = Database.Get(docId,GetRequestTransaction(context));
					if (doc == null)
					{
						context.SetStatusToNotFound();
						return;
					}
					if(doc.NonAuthoritiveInformation)
					{
						context.SetStatusToNonAuthoritiveInformation();
					}
					if (context.MatchEtag(doc.Etag))
					{
						context.SetStatusToNotModified();
						return;
					}
					doc.Metadata["Last-Modified"] = doc.LastModified.ToString("r");
					context.WriteData(doc.DataAsJson, doc.Metadata, doc.Etag);
					break;
				case "DELETE":
					Database.Delete(docId, context.GetEtag(), GetRequestTransaction(context));
					context.SetStatusToDeleted();
					break;
				case "PUT":
					Put(context, docId);
					break;
				case "PATCH":
					var patchRequestJson = context.ReadJsonArray();
					var patchRequests = patchRequestJson.Cast<JObject>().Select(PatchRequest.FromJson).ToArray();
					var patchResult = Database.ApplyPatch(docId, context.GetEtag(), patchRequests, GetRequestTransaction(context));
					switch (patchResult)
					{
						case PatchResult.DocumentDoesNotExists:
							context.SetStatusToNotFound();
							break;
						case PatchResult.Patched:
							context.Response.Headers["Location"] = Database.Configuration.GetFullUrl("/docs/" + docId);
							context.WriteJson(new {Patched = true});
							break;
						default:
							throw new ArgumentOutOfRangeException("Value " + patchResult + " is not understood");
					}
					break;
			}
		}

		private void Put(IHttpContext context, string docId)
		{
			var json = context.ReadJson();
			context.SetStatusToCreated("/docs/" + docId);
			var putResult = Database.Put(docId, context.GetEtag(), json, context.Request.Headers.FilterHeaders(isServerDocument: true), GetRequestTransaction(context));
            context.WriteJson(putResult);
		}
	}
}