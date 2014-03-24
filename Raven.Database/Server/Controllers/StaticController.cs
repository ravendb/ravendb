using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Extensions;

namespace Raven.Database.Server.Controllers
{
	[RoutePrefix("")]
	public class StaticController : RavenDbApiController
	{
		[HttpGet]
		[Route("static/")]
		[Route("databases/{databaseName}/static/")]
		public HttpResponseMessage StaticGet()
		{
			var array = Database.Attachments.GetAttachments(GetStart(),
										   GetPageSize(Database.Configuration.MaxPageSize),
										   GetEtagFromQueryString(),
										   GetQueryStringValue("startsWith"),
										   long.MaxValue);

			return GetMessageWithObject(array);
		}

		[HttpGet]
		[Route("static/{*id}")]
		[Route("databases/{databaseName}/static/{*id}")]
		public HttpResponseMessage StaticGet(string id)
		{
            if (id == null)
		        return StaticGet();

			var filename = id;
			var result = GetEmptyMessage();
			Database.TransactionalStorage.Batch(_ => // have to keep the session open for reading of the attachment stream
			{
				var attachmentAndHeaders = Database.Attachments.GetStatic(filename);
				if (attachmentAndHeaders == null)
				{
					result = GetEmptyMessage(HttpStatusCode.NotFound);
					return;
				}

				if (MatchEtag(attachmentAndHeaders.Etag))
				{
					result = GetEmptyMessage(HttpStatusCode.NotModified);
					return;
				}

				result.Content = new PushStreamContent((outputStream, __, ___) =>
					Database.TransactionalStorage.Batch(accessor =>
					{
						using(outputStream)
						using (var stream = attachmentAndHeaders.Data())
						{
							stream.CopyTo(outputStream);
							outputStream.Flush();
						}
					}));

				WriteHeaders(attachmentAndHeaders.Metadata, attachmentAndHeaders.Etag, result);

			});

			return result;
		}

		[HttpHead]
		[Route("static/{*id}")]
		[Route("databases/{databaseName}/static/{*id}")]
		public HttpResponseMessage StaticHead(string id)
		{
			var filename = id;
			var result = GetEmptyMessage();
			Database.TransactionalStorage.Batch(_ => // have to keep the session open for reading of the attachment stream
			{
				var attachmentAndHeaders = Database.Attachments.GetStatic(filename);
				if (attachmentAndHeaders == null)
				{
					result = GetEmptyMessage(HttpStatusCode.NotFound);
					return;
				}
				if (MatchEtag(attachmentAndHeaders.Etag))
				{
					result = GetEmptyMessage(HttpStatusCode.NotModified);
					return;
				}

				result.Content = new StaticHeadContent(attachmentAndHeaders.Size);
				WriteHeaders(attachmentAndHeaders.Metadata, attachmentAndHeaders.Etag, result);
			});

			return result;
		}

		[HttpPut]
		[Route("static/{*filename}")]
		[Route("databases/{databaseName}/static/{*filename}")]
		public async Task<HttpResponseMessage> StaticPut(string filename)
		{
			var newEtag = Database.Attachments.PutStatic(filename, GetEtag(), await InnerRequest.Content.ReadAsStreamAsync(), InnerHeaders.FilterHeadersAttachment());

			var msg = GetEmptyMessage(HttpStatusCode.Created);
			msg.Headers.Location = Database.Configuration.GetFullUrl("static/" + filename);

			WriteETag(newEtag, msg);
			return msg;
		}

		[HttpPost]
		[Route("static/{*id}")]
		[Route("databases/{databaseName}/static/{*id}")]
		public HttpResponseMessage StaticPost(string id)
		{
			var filename = id;
			var newEtagPost = Database.Attachments.PutStatic(filename, GetEtag(), null, InnerHeaders.FilterHeadersAttachment());

			var msg = GetMessageWithObject(newEtagPost);
			WriteETag(newEtagPost, msg);
			return msg;
		}

		[HttpDelete]
		[Route("static/{*id}")]
		[Route("databases/{databaseName}/static/{*id}")]
		public HttpResponseMessage StaticDelete(string id)
		{
			var filename = id;
			Database.Attachments.DeleteStatic(filename, GetEtag());
			return GetEmptyMessage(HttpStatusCode.NoContent);
		}
	}
}