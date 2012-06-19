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
	public class Document : RequestResponder
	{
		public override string UrlPattern
		{
			get { return @"^/docs/(.+)"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] {"GET", "DELETE", "PUT", "PATCH", "HEAD"}; }
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
					var patchRequestJson = context.ReadJsonArray();
					var patchRequests = patchRequestJson.Cast<RavenJObject>().Select(PatchRequest.FromJson).ToArray();
					var patchResult = Database.ApplyPatch(docId, context.GetEtag(), patchRequests, GetRequestTransaction(context));
					switch (patchResult)
					{
						case PatchResult.DocumentDoesNotExists:
							context.SetStatusToNotFound();
							break;
						case PatchResult.Patched:
							context.Response.AddHeader("Location", Database.Configuration.GetFullUrl("/docs/" + docId));
							context.WriteJson(new {Patched = true});
							break;
						default:
							throw new ArgumentOutOfRangeException("Value " + patchResult + " is not understood");
					}
					break;
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

			Database.TransactionalStorage.Batch(
				_ => // we are running this here to ensure transactional safety for the two operations
				{
					var transactionInformation = GetRequestTransaction(context);
					var documentMetadata = Database.GetDocumentMetadata(docId, transactionInformation);
					if (documentMetadata == null)
					{
						context.SetStatusToNotFound();
						return;
					}
					Debug.Assert(documentMetadata.Etag != null);
					if (context.MatchEtag(documentMetadata.Etag.Value) && documentMetadata.NonAuthoritativeInformation == false)
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
			if (context.MatchEtag(documentMetadata.Etag.Value) && documentMetadata.NonAuthoritativeInformation == false)
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
			context.WriteHeaders(documentMetadata.Metadata, documentMetadata.Etag.Value);
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
			doc.Metadata[Constants.DocumentIdFieldName] = doc.Key;
			context.WriteData(doc.DataAsJson, doc.Metadata, doc.Etag.Value);
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
