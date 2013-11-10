using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Extensions;

namespace Raven.Database.Server.Controllers
{
	[RoutePrefix("")]
	public class StaticController : RavenApiController
	{
		[HttpGet][Route("static")]
		public HttpResponseMessage StaticGet()
		{
			var array = Database.GetAttachments(GetStart(),
										   GetPageSize(Database.Configuration.MaxPageSize),
										   GetEtagFromQueryString(),
										   GetQueryStringValue("startsWith"),
										   long.MaxValue);

			return GetMessageWithObject(array);
		}

		[HttpGet][Route("static/{*id}")]
		public HttpResponseMessage StaticGet(string id)
		{
			var filename = id;
			var result = GetEmptyMessage();
			Database.TransactionalStorage.Batch(_ => // have to keep the session open for reading of the attachment stream
			{
				var attachmentAndHeaders = Database.GetStatic(filename);
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

		[HttpHead][Route("static/{*id}")]
		public HttpResponseMessage StaticHead(string id)
		{
			var filename = id;
			var result = GetEmptyMessage();
			Database.TransactionalStorage.Batch(_ => // have to keep the session open for reading of the attachment stream
			{
				var attachmentAndHeaders = Database.GetStatic(filename);
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

				WriteHeaders(attachmentAndHeaders.Metadata, attachmentAndHeaders.Etag, result);
				//TODO: set length
				//context.Response.ContentLength64 = attachmentAndHeaders.Size;
			});

			return result;
		}

		[HttpPut][Route("static/{*id}")]
		public async Task<HttpResponseMessage> StaticPut(string id)
		{
			var filename = id;

			var newEtag = Database.PutStatic(filename, GetEtag(), await InnerRequest.Content.ReadAsStreamAsync(), InnerHeaders.FilterHeadersAttachment());

			var msg = GetEmptyMessage(HttpStatusCode.NoContent);

			WriteETag(newEtag, msg);
			return msg;
		}

		[HttpPost][Route("static/{*id}")]
		public HttpResponseMessage StaticPost(string id)
		{
			var filename = id;
			var newEtagPost = Database.PutStatic(filename, GetEtag(), null, InnerHeaders.FilterHeadersAttachment());

			var msg = GetMessageWithObject(newEtagPost);
			WriteETag(newEtagPost, msg);
			return msg;
		}

		[HttpDelete][Route("static/{*id}")]
		public HttpResponseMessage StaticDelete(string id)
		{
			var filename = id;
			Database.DeleteStatic(filename, GetEtag());
			return GetEmptyMessage(HttpStatusCode.NoContent);
		}
	}
}