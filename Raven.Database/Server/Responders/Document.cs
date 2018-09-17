//-----------------------------------------------------------------------
// <copyright file="Document.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Diagnostics;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Json;
using System.Linq;
using Raven.Database.Server.Abstractions;
using Raven.Json.Linq;

namespace Raven.Database.Server.Responders
{
	public class Document : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return @"^/docs/(.+)"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] {"GET", "DELETE", "PUT", "PATCH", "EVAL", "HEAD"}; }
		}

		public override void Respond(IHttpContext context)
		{
			var match = urlMatcher.Match(context.GetRequestUrl());
			var docId = Uri.UnescapeDataString(match.Groups[1].Value);
			switch (context.Request.HttpMethod)
			{
				case "HEAD":
					Head(context, docId);
					break;
				case "GET":
					Get(context, docId);
					break;
				case "DELETE":
					Database.Delete(docId, context.GetEtag(), GetRequestTransaction(context));
					context.SetStatusToDeleted();
					break;
				case "PUT":
					Put(context, docId);
					break;
				case "PATCH":
					long _;
					var patchRequestJson = context.ReadJsonArray(out _);
					var patchRequests = patchRequestJson.Cast<RavenJObject>().Select(PatchRequest.FromJson).ToArray();
					var patchResult = Database.ApplyPatch(docId, context.GetEtag(), patchRequests, GetRequestTransaction(context));
					ProcessPatchResult(context, docId, patchResult.PatchResult, null, null);
					break;
				case "EVAL":
					var advPatchRequestJson = context.ReadJsonObject<RavenJObject>();
					var advPatch = ScriptedPatchRequest.FromJson(advPatchRequestJson);
					bool testOnly;
					bool.TryParse(context.Request.QueryString["test"], out testOnly);
					var advPatchResult = Database.ApplyPatch(docId, context.GetEtag(), advPatch, GetRequestTransaction(context), testOnly);
					ProcessPatchResult(context, docId, advPatchResult.Item1.PatchResult, advPatchResult.Item2, advPatchResult.Item1.Document);
					break;
			}
		}

		private void ProcessPatchResult(IHttpContext context, string docId, PatchResult patchResult, object debug, RavenJObject document)
		{
				switch (patchResult)
				{
					case PatchResult.DocumentDoesNotExists:
						context.SetStatusToNotFound();
						break;
					case PatchResult.Patched:
						context.Response.AddHeader("Location", Database.Configuration.GetFullUrl("/docs/" + docId));
						context.WriteJson(new {Patched = true, Debug = debug});
						break;
					case PatchResult.Tested:
						context.WriteJson(new
						{
							Patched = false, 
							Debug = debug,
							Document = document
						});
						break;
					default:
						throw new ArgumentOutOfRangeException("Value " + patchResult + " is not understood");
				}
		}

		private void Get(IHttpContext context, string docId)
		{
			context.Response.AddHeader("Content-Type", "application/json; charset=utf-8");
			if (string.IsNullOrEmpty(context.Request.Headers["If-None-Match"]))
			{
				GetDocumentDirectly(context, docId);
				return;
			}

			var transactionInformation = GetRequestTransaction(context);

			var nonAuthoritativeInformationBehavior =
				Database.InFlightTransactionalState.GetNonAuthoritativeInformationBehavior<JsonDocumentMetadata>(
					transactionInformation, docId);
          
			Database.TransactionalStorage.Batch(
				_ => // we are running this here to ensure transactional safety for the two operations
				{
					var documentMetadata = Database.GetDocumentMetadata(docId, transactionInformation);
					if (nonAuthoritativeInformationBehavior != null)
						nonAuthoritativeInformationBehavior(documentMetadata);
					if (documentMetadata == null)
					{
						context.SetStatusToNotFound();
						return;
					}
					Debug.Assert(documentMetadata.Etag != null);
					if (context.MatchEtag(documentMetadata.Etag) &&
						documentMetadata.NonAuthoritativeInformation != true)
					{
						context.SetStatusToNotModified();
						return;
					}
					if (documentMetadata.NonAuthoritativeInformation != null && documentMetadata.NonAuthoritativeInformation.Value)
					{
						context.SetStatusToNonAuthoritativeInformation();
					}

					GetDocumentDirectly(context, docId);
				});
		}

		private void Head(IHttpContext context, string docId)
		{
			context.Response.AddHeader("Content-Type", "application/json; charset=utf-8");

			var transactionInformation = GetRequestTransaction(context);
			var documentMetadata = Database.GetDocumentMetadata(docId, transactionInformation);
			if (documentMetadata == null)
			{
				context.SetStatusToNotFound();
				return;
			}
			Debug.Assert(documentMetadata.Etag != null);
			if (context.MatchEtag(documentMetadata.Etag) && documentMetadata.NonAuthoritativeInformation == false)
			{
				context.SetStatusToNotModified();
				return;
			}

			if (documentMetadata.NonAuthoritativeInformation != null && documentMetadata.NonAuthoritativeInformation.Value)
			{
				context.SetStatusToNonAuthoritativeInformation();
			}
			documentMetadata.Metadata[Constants.DocumentIdFieldName] = documentMetadata.Key;
			documentMetadata.Metadata[Constants.LastModified] = documentMetadata.LastModified; //HACK ? to get the document's last modified value into the response headers
			context.WriteHeaders(documentMetadata.Metadata, documentMetadata.Etag);
		}

		private void GetDocumentDirectly(IHttpContext context, string docId)
		{
			var doc = Database.Get(docId, GetRequestTransaction(context));
			if (doc == null)
			{
				context.SetStatusToNotFound();
				return;
			}
			if (doc.NonAuthoritativeInformation != null && doc.NonAuthoritativeInformation.Value)
			{
				context.SetStatusToNonAuthoritativeInformation();
			}
			Debug.Assert(doc.Etag != null);
			doc.Metadata[Constants.LastModified] = doc.LastModified;
			doc.Metadata[Constants.DocumentIdFieldName] = Uri.EscapeUriString(doc.Key ?? string.Empty);
			context.WriteData(doc.DataAsJson, doc.Metadata, doc.Etag);
		}

		private void Put(IHttpContext context, string docId)
		{
			var json = context.ReadJson();
			context.SetStatusToCreated("/docs/" + Uri.EscapeUriString(docId));
			var putResult = Database.Put(docId, context.GetEtag(), json, context.Request.Headers.FilterHeaders(), GetRequestTransaction(context));
			context.WriteJson(putResult);
		}
	}
}
