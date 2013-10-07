using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Web.Http;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Database.Impl;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers
{
	public class StreamsController : RavenApiController
	{
		[HttpGet("streams/docs")]
		[HttpGet("databases/{databaseName}/streams/docs")]
		public HttpResponseMessage StreamDocsGet()
		{
			var start = GetStart();
			var etag = GetEtagFromQueryString();
			var startsWith = GetQueryStringValue("startsWith");
			int pageSize = GetPageSize(int.MaxValue);
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
						ContentType = new MediaTypeHeaderValue("application/json")
						{
							CharSet = "utf-8"
						}
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
						{
							Database.GetDocuments(start, pageSize, etag,
								doc => doc.WriteTo(writer));
						}
						else
						{
							Database.GetDocumentsWithIdStartingWith(startsWith, matches, null,
								start, pageSize, doc => doc.WriteTo(writer));
						}
					}
				});

				writer.WriteEndArray();
				writer.WriteEndObject();
				writer.Flush();
			}
		}

		private void StreamToClientQuery(Stream stream, string index, IndexQuery query, bool isHeadRequest, HttpResponseMessage msg)
		{
			using (var writer = GetOutputWriter(msg, stream))
			{
				// we may be sending a LOT of documents to the user, and most 
				// of them aren't going to be relevant for other ops, so we are going to skip
				// the cache for that, to avoid filling it up very quickly
				using (DocumentCacher.SkipSettingDocumentsInDocumentCache())
				{
					Database.Query(index, query, information =>
					{
						msg.Headers.Add("Raven-Result-Etag", information.ResultEtag.ToString());
						msg.Headers.Add("Raven-Index-Etag", information.IndexEtag.ToString());
						msg.Headers.Add("Raven-Is-Stale", information.IsStable ? "true" : "false");
						msg.Headers.Add("Raven-Index", information.Index);
						msg.Headers.Add("Raven-Total-Results", information.TotalResults.ToString(CultureInfo.InvariantCulture));
						msg.Headers.Add("Raven-Index-Timestamp",
							information.IndexTimestamp.ToString(Default.DateTimeFormatsToWrite,
								CultureInfo.InvariantCulture));

						if (isHeadRequest)
							return;
						writer.WriteHeader();
					}, writer.Write);
				}
			}
		}

		[HttpGet("streams/query/{*id}")]
		[HttpGet("databases/{databaseName}/streams/query/{*id}")]
		public HttpResponseMessage SteamQueryGet(string id)
		{
			var msg = GetMessageWithString("");

			var index = id;
			var query = GetIndexQuery(int.MaxValue);
			if (string.IsNullOrEmpty(GetQueryStringValue("pageSize")))
				query.PageSize = int.MaxValue;
			var isHeadRequest = Request.Method == HttpMethod.Head;
			if (isHeadRequest)
				query.PageSize = 0;

			msg.Content = new PushStreamContent((stream, content, arg3) => StreamToClientQuery(stream, index, query, isHeadRequest, msg));
			msg.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json") { CharSet = "utf-8" };

			return msg;
		}

		private IOutputWriter GetOutputWriter(HttpResponseMessage msg, Stream stream)
		{
			var useExcelFormat = "excel".Equals(GetQueryStringValue("format"), StringComparison.InvariantCultureIgnoreCase);
			if (useExcelFormat)
				return new ExcelOutputWriter(msg, stream);
			return new JsonOutputWriter(stream);
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

			public ExcelOutputWriter(HttpResponseMessage msg, Stream stream)
			{
				this.stream = stream;
				msg.Content.Headers.ContentType = new MediaTypeHeaderValue("text/csv, application/vnd.msexcel, text/anytext");
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
				writer = new StreamWriter(stream);
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
				{
					writer.Write('"');
				}
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