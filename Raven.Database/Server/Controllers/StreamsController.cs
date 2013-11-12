using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Database.Impl;
using Raven.Database.Storage;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers
{
	public class StreamsController : RavenApiController
	{
		[HttpGet]
		[Route("streams/docs")]
		[Route("databases/{databaseName}/streams/docs")]
		public HttpResponseMessage StreamDocsGet()
		{
			var start = GetStart();
			var etag = GetEtagFromQueryString();
			var startsWith = GetQueryStringValue("startsWith");
			var pageSize = GetPageSize(int.MaxValue);
			var matches = GetQueryStringValue("matches");
			if (string.IsNullOrEmpty(GetQueryStringValue("pageSize")))
				pageSize = int.MaxValue;

			return new HttpResponseMessage(HttpStatusCode.OK)
			{
				Content = new PushStreamContent((stream, content, transportContext) =>
					StreamToClient(stream, startsWith, start, pageSize, etag, matches))
				{
					Headers =
					{
						ContentType = new MediaTypeHeaderValue("application/json"){CharSet = "utf-8"}
					}
				}
			};
		}

		private void StreamToClient(Stream stream, string startsWith, int start, int pageSize, Etag etag, string matches)
		{
			using (var writer = new JsonTextWriter(new StreamWriter(stream)))
			{
				writer.WriteStartObject();
				writer.WritePropertyName("Results");
				writer.WriteStartArray();

				Database.TransactionalStorage.Batch(accessor =>
				{
					// we may be sending a LOT of documents to the user, and most 
					// of them aren't going to be relevant for other ops, so we are going to skip
					// the cache for that, to avoid filling it up very quickly
					using (DocumentCacher.SkipSettingDocumentsInDocumentCache())
					{
						if (string.IsNullOrEmpty(startsWith))
							Database.GetDocuments(start, pageSize, etag, doc => doc.WriteTo(writer));
						else
							Database.GetDocumentsWithIdStartingWith(startsWith, matches, null, start, pageSize, doc => doc.WriteTo(writer));
					}
				});

				writer.WriteEndArray();
				writer.WriteEndObject();
				writer.Flush();
			}
		}

		[HttpGet]
		[Route("streams/query/{*id}")]
		[Route("databases/{databaseName}/streams/query/{*id}")]
		public HttpResponseMessage SteamQueryGet(string id)
		{
			var msg = GetEmptyMessage();

			var index = id;
			var query = GetIndexQuery(int.MaxValue);
			if (string.IsNullOrEmpty(GetQueryStringValue("pageSize")))
				query.PageSize = int.MaxValue;
			var isHeadRequest = InnerRequest.Method == HttpMethod.Head;
			if (isHeadRequest)
				query.PageSize = 0;

			var accessor = Database.TransactionalStorage.CreateAccessor();

			try
			{
				var queryOp = new DocumentDatabase.DatabaseQueryOperation(Database, index, query, accessor);
				queryOp.Init();

				msg.Content = new StreamQueryContent(InnerRequest, queryOp, accessor);
				msg.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json") { CharSet = "utf-8" };
				msg.Headers.Add("Raven-Result-Etag", queryOp.Header.ResultEtag.ToString());
				msg.Headers.Add("Raven-Index-Etag", queryOp.Header.IndexEtag.ToString());
				msg.Headers.Add("Raven-Is-Stale", queryOp.Header.IsStable ? "true" : "false");
				msg.Headers.Add("Raven-Index", queryOp.Header.Index);
				msg.Headers.Add("Raven-Total-Results", queryOp.Header.TotalResults.ToString(CultureInfo.InvariantCulture));
				msg.Headers.Add("Raven-Index-Timestamp",
					queryOp.Header.IndexTimestamp.ToString(Default.DateTimeFormatsToWrite, CultureInfo.InvariantCulture));

			}
			catch (Exception)
			{
				accessor.Dispose();
				throw;
			}

			return msg;
		}

		public class StreamQueryContent : HttpContent
		{
			private readonly HttpRequestMessage req;
			private readonly DocumentDatabase.DatabaseQueryOperation queryOp;
			private readonly IStorageActionsAccessor accessor;

			public StreamQueryContent(HttpRequestMessage req,DocumentDatabase.DatabaseQueryOperation queryOp, IStorageActionsAccessor accessor)
			{
				this.req = req;
				this.queryOp = queryOp;
				this.accessor = accessor;
			}

			protected override Task SerializeToStreamAsync(Stream stream, TransportContext context)
			{
				using (var writer = GetOutputWriter(req, stream))
				{
					writer.WriteHeader();
					queryOp.Execute(writer.Write);
				}
				return Task.FromResult(true);
			}

			protected override bool TryComputeLength(out long length)
			{
				length = -1;
				return false;
			}

			protected override void Dispose(bool disposing)
			{
				base.Dispose(disposing);
				accessor.Dispose();
				queryOp.Dispose();
			}
		}

		private static IOutputWriter GetOutputWriter(HttpRequestMessage req, Stream stream)
		{
			var useExcelFormat = "excel".Equals(GetQueryStringValue(req, "format"), StringComparison.InvariantCultureIgnoreCase);
			return useExcelFormat ? (IOutputWriter) new ExcelOutputWriter(stream) : new JsonOutputWriter(stream);
		}

		public interface IOutputWriter : IDisposable
		{
			void WriteHeader();
			void Write(RavenJObject result);
		}

		private class ExcelOutputWriter : IOutputWriter
		{
			private readonly Stream stream;
			private StreamWriter writer;

			public ExcelOutputWriter( Stream stream)
			{
				this.stream = stream;
				// TODO, make this work
				//msg.Content.Headers.ContentType = new MediaTypeHeaderValue("text/csv, application/vnd.msexcel, text/anytext");
			}

			public void Dispose()
			{
				if (writer == null)
					return;

				writer.Flush();
				writer.Close();
			}

			public void WriteHeader()
			{
				writer = new StreamWriter(stream, Encoding.UTF8);
			}

			public void Write(RavenJObject result)
			{
				if (properties == null)
				{
					GetPropertiesAndWriteCsvHeader(result);
					Debug.Assert(properties != null);
				}

				foreach (var property in properties)
				{
					var token = result.SelectToken(property);
					if (token != null)
					{
						switch (token.Type)
						{
							case JTokenType.Null:
								break;

							case JTokenType.Array:
							case JTokenType.Object:
								OutputCsvValue(token.ToString(Formatting.None));
								break;

							default:
								OutputCsvValue(token.Value<string>());
								break;
						}
					}

					writer.Write(',');
				}

				writer.WriteLine();
			}

			private void GetPropertiesAndWriteCsvHeader(RavenJObject result)
			{
				properties = DocumentHelpers.GetPropertiesFromJObject(result,
					parentPropertyPath: "",
					includeNestedProperties: true,
					includeMetadata: false,
					excludeParentPropertyNames: true);

				foreach (var property in properties)
				{
					OutputCsvValue(property);
					writer.Write(',');
				}
				writer.WriteLine();
			}

			private static readonly char[] RequireQuotesChars = { ',', '\r', '\n', '"' };
			private IEnumerable<string> properties;

			private void OutputCsvValue(string val)
			{
				var needsQuoutes = val.IndexOfAny(RequireQuotesChars) != -1;
				if (needsQuoutes)
					writer.Write('"');

				writer.Write(needsQuoutes ? val.Replace("\"", "\"\"") : val);
				if (needsQuoutes)
					writer.Write('"');
			}
		}

		public class JsonOutputWriter : IOutputWriter
		{
			private readonly Stream stream;
			private JsonWriter writer;

			public JsonOutputWriter(Stream stream)
			{
				this.stream = stream;
			}

			public void WriteHeader()
			{
				writer = new JsonTextWriter(new StreamWriter(stream));
				writer.WriteStartObject();
				writer.WritePropertyName("Results");
				writer.WriteStartArray();
			}

			public void Dispose()
			{
				if (writer == null)
					return;

				writer.WriteEndArray();
				writer.WriteEndObject();

				writer.Flush();
				writer.Close();
			}

			public void Write(RavenJObject result)
			{
				result.WriteTo(writer, Default.Converters);
			}
		}
	}
}