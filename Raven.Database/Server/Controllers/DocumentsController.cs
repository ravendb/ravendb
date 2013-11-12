using System;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers
{
	public class DocumentsController : RavenApiController
	{
		[HttpGet]
		[Route("docs")]
		[Route("databases/{databaseName}/docs")]
		public HttpResponseMessage DocsGet()
		{
			long documentsCount = 0;
			var lastDocEtag = Etag.Empty;
			Database.TransactionalStorage.Batch(accessor =>
			{
				lastDocEtag = accessor.Staleness.GetMostRecentDocumentEtag();
				documentsCount = accessor.Documents.GetDocumentsCount();
			});

			lastDocEtag = lastDocEtag.HashWith(BitConverter.GetBytes(documentsCount));
			if (MatchEtag(lastDocEtag))
				return GetEmptyMessage(HttpStatusCode.NotModified);

			var startsWith = GetQueryStringValue("startsWith");
			HttpResponseMessage msg;
			if (string.IsNullOrEmpty(startsWith))
				msg = GetMessageWithObject(Database.GetDocuments(GetStart(), GetPageSize(Database.Configuration.MaxPageSize),
					GetEtagFromQueryString()));
			else
				msg = GetMessageWithObject(Database.GetDocumentsWithIdStartingWith(startsWith, GetQueryStringValue("matches"), null,
					GetStart(), GetPageSize(Database.Configuration.MaxPageSize)));
			WriteHeaders(new RavenJObject(), lastDocEtag, msg);
			return msg;
		}

		[HttpPost]
		[Route("docs")]
		[Route("databases/{databaseName}/docs")]
		public async Task<HttpResponseMessage> DocsPost()
		{
			var json = await ReadJsonAsync();
			var id = Database.Put(null, Etag.Empty, json,
								  InnerHeaders.FilterHeaders(),
								  GetRequestTransaction());

			return GetMessageWithObject(id);
		}

		[HttpHead]
		[Route("docs/{*id}")]
		[Route("databases/{databaseName}/docs/{*id}")]
		public HttpResponseMessage DocHead(string id)
		{
			var msg = GetEmptyMessage();
			msg.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json") { CharSet = "utf-8" };
			var docId = id;
			var transactionInformation = GetRequestTransaction();
			var documentMetadata = Database.GetDocumentMetadata(docId, transactionInformation);
			if (documentMetadata == null)
			{
				msg.StatusCode = HttpStatusCode.NotFound;
				return msg;
			}

			Debug.Assert(documentMetadata.Etag != null);
			if (MatchEtag(documentMetadata.Etag) && documentMetadata.NonAuthoritativeInformation == false)
			{
				msg.StatusCode = HttpStatusCode.NotModified;
				return msg;
			}

			if (documentMetadata.NonAuthoritativeInformation != null && documentMetadata.NonAuthoritativeInformation.Value)
				msg.StatusCode = HttpStatusCode.NonAuthoritativeInformation;

			documentMetadata.Metadata[Constants.DocumentIdFieldName] = documentMetadata.Key;
			documentMetadata.Metadata[Constants.LastModified] = documentMetadata.LastModified; //HACK ? to get the document's last modified value into the response headers

			WriteHeaders(documentMetadata.Metadata, documentMetadata.Etag, msg);

			return msg;
		}

		[HttpGet]
		[Route("docs/{*id}")]
		[Route("databases/{databaseName}/docs/{*id}")]
		public HttpResponseMessage DocGet(string id)
		{
			var docId = id;
			var msg = GetEmptyMessage();
			if (string.IsNullOrEmpty(GetHeader("If-None-Match")))
				return GetDocumentDirectly(docId, msg);

			Database.TransactionalStorage.Batch(
				_ => // we are running this here to ensure transactional safety for the two operations
				{
					var transactionInformation = GetRequestTransaction();
					var documentMetadata = Database.GetDocumentMetadata(docId, transactionInformation);
					if (documentMetadata == null)
					{
						msg = GetEmptyMessage(HttpStatusCode.NotFound);
						return;
					}
					Debug.Assert(documentMetadata.Etag != null);
					if (MatchEtag(documentMetadata.Etag) && documentMetadata.NonAuthoritativeInformation != true)
					{
						msg.StatusCode = HttpStatusCode.NotModified;
						return;
					}
					if (documentMetadata.NonAuthoritativeInformation != null && documentMetadata.NonAuthoritativeInformation.Value)
						msg.StatusCode = HttpStatusCode.NonAuthoritativeInformation;

					msg = GetDocumentDirectly(docId, msg);
				});

			return msg;
		}

		[HttpDelete]
		[Route("docs/{*id}")]
		[Route("databases/{databaseName}/docs/{*id}")]
		public HttpResponseMessage DocDelete(string id)
		{
			var docId = id;
			Database.Delete(docId, GetEtag(), GetRequestTransaction());
			return GetEmptyMessage(HttpStatusCode.NoContent);
		}

		[HttpPut]
		[Route("docs/{*id}")]
		[Route("databases/{databaseName}/docs/{*id}")]
		public async Task<HttpResponseMessage> DocPut(string id)
		{
			var docId = id;
			var json = await ReadJsonAsync();
			var putResult = Database.Put(docId, GetEtag(), json, InnerHeaders.FilterHeaders(), GetRequestTransaction());
			return GetMessageWithObject(putResult, HttpStatusCode.Created);
		}

		[HttpPatch]
		[Route("docs/{*id}")]
		[Route("databases/{databaseName}/docs/{*id}")]
		public async Task<HttpResponseMessage> DocPatch(string id)
		{
			var docId = id;
			var patchRequestJson = await ReadJsonArrayAsync();
			var patchRequests = patchRequestJson.Cast<RavenJObject>().Select(PatchRequest.FromJson).ToArray();
			var patchResult = Database.ApplyPatch(docId, GetEtag(), patchRequests, GetRequestTransaction());
			return ProcessPatchResult(docId, patchResult.PatchResult, null, null);
		}

		[HttpEval]
		[Route("docs/{*id}")]
		[Route("databases/{databaseName}/docs/{*id}")]
		public async Task<HttpResponseMessage> DocEval(string id)
		{
			var docId = id;
			var advPatchRequestJson = await ReadJsonObjectAsync<RavenJObject>();
			var advPatch = ScriptedPatchRequest.FromJson(advPatchRequestJson);
			bool testOnly;
			bool.TryParse(GetQueryStringValue("test"), out testOnly);
			var advPatchResult = Database.ApplyPatch(docId, GetEtag(), advPatch, GetRequestTransaction(), testOnly);
			return ProcessPatchResult(docId, advPatchResult.Item1.PatchResult, advPatchResult.Item2, advPatchResult.Item1.Document);
		}

		private HttpResponseMessage GetDocumentDirectly(string docId, HttpResponseMessage msg)
		{
			if (Database == null)
			{
				msg.StatusCode = HttpStatusCode.NotFound;
				return msg;
			}
			var doc = Database.Get(docId, GetRequestTransaction());
			if (doc == null)
			{
				msg.StatusCode = HttpStatusCode.NotFound;
				return msg;
			}

			if (doc.NonAuthoritativeInformation != null && doc.NonAuthoritativeInformation.Value)
				msg.StatusCode = HttpStatusCode.NonAuthoritativeInformation;

			Debug.Assert(doc.Etag != null);
			doc.Metadata[Constants.LastModified] = doc.LastModified;
			doc.Metadata[Constants.DocumentIdFieldName] = Uri.EscapeUriString(doc.Key ?? string.Empty);
			msg.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json") { CharSet = "utf-8" };
			return WriteData(doc.DataAsJson, doc.Metadata, doc.Etag, msg: msg);
		}

		private HttpResponseMessage ProcessPatchResult(string docId, PatchResult patchResult, object debug, RavenJObject document)
		{
			switch (patchResult)
			{
				case PatchResult.DocumentDoesNotExists:
					return GetEmptyMessage(HttpStatusCode.NotFound);
				case PatchResult.Patched:
					var msg = GetMessageWithObject(new { Patched = true, Debug = debug });
					msg.Headers.Add("Location", Database.Configuration.GetFullUrl("/docs/" + docId));
					return msg;
				case PatchResult.Tested:
					return GetMessageWithObject(new
					{
						Patched = false,
						Debug = debug,
						Document = document
					});
				default:
					throw new ArgumentOutOfRangeException("Value " + patchResult + " is not understood");
			}
		}
	}
}