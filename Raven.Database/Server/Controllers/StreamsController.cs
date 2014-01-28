using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Authentication.ExtendedProtection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Storage;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;
using System.Linq;

namespace Raven.Database.Server.Controllers
{
	public class StreamsController : RavenDbApiController
	{
		[HttpGet]
		[Route("streams/docs")]
		[Route("databases/{databaseName}/streams/docs")]
		public HttpResponseMessage StreamDocsGet()
		{
            using (var cts = new CancellationTokenSource())
            using (var timeout = cts.TimeoutAfter(DatabasesLandlord.SystemConfiguration.DatbaseOperationTimeout))
            {
                var start = GetStart();
                var etag = GetEtagFromQueryString();
                var startsWith = GetQueryStringValue("startsWith");
                var pageSize = GetPageSize(int.MaxValue);
                var matches = GetQueryStringValue("matches");
                var nextPageStart = GetNextPageStart();
                if (string.IsNullOrEmpty(GetQueryStringValue("pageSize")))
                    pageSize = int.MaxValue;

                return new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new PushStreamContent((stream, content, transportContext) =>
                        StreamToClient(stream, startsWith, start, pageSize, etag, matches, nextPageStart, cts.Token, timeout))
                    {
                        Headers =
                        {
                            ContentType = new MediaTypeHeaderValue("application/json") { CharSet = "utf-8" }
                        }
                    }
                };
            }
		}

		private void StreamToClient(Stream stream, string startsWith, int start, int pageSize, Etag etag, string matches, int nextPageStart, CancellationToken token, CancellationTokenSourceExtensions.CancellationTimeout timeout)
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
							Database.GetDocuments(start, pageSize, etag, token, doc =>
							{
                                timeout.Delay();
							    doc.WriteTo(writer);
							});
						else
						{
						    var nextPageStartInternal = nextPageStart;

                            Database.GetDocumentsWithIdStartingWith(startsWith, matches, null, start, pageSize, token, ref nextPageStartInternal, doc =>
                            {
                                timeout.Delay();
                                doc.WriteTo(writer);
                            });

						    nextPageStart = nextPageStartInternal;
						}
					}
				});

				writer.WriteEndArray();
                writer.WritePropertyName("NextPageStart");
                writer.WriteValue(nextPageStart);
				writer.WriteEndObject();
				writer.Flush();
			}
		}

		[HttpGet]
		[Route("streams/query/{*id}")]
		[Route("databases/{databaseName}/streams/query/{*id}")]
		public HttpResponseMessage SteamQueryGet(string id)
		{
            using (var cts = new CancellationTokenSource())
            using (cts.TimeoutAfter(DatabasesLandlord.SystemConfiguration.DatbaseOperationTimeout))
            {
                var msg = GetEmptyMessage();

                var index = id;
                var query = GetIndexQuery(int.MaxValue);
                if (string.IsNullOrEmpty(GetQueryStringValue("pageSize"))) query.PageSize = int.MaxValue;
                var isHeadRequest = InnerRequest.Method == HttpMethod.Head;
                if (isHeadRequest) query.PageSize = 0;

			var accessor = Database.TransactionalStorage.CreateAccessor(); //accessor will be disposed in the StreamQueryContent!

                try
                {
                    var queryOp = new DocumentDatabase.DatabaseQueryOperation(Database, index, query, accessor);
                    queryOp.Init();
				msg.Content = new StreamQueryContent(InnerRequest, queryOp, accessor,
					mediaType => msg.Content.Headers.ContentType = new MediaTypeHeaderValue(mediaType) { CharSet = "utf-8" });

                    msg.Headers.Add("Raven-Result-Etag", queryOp.Header.ResultEtag.ToString());
                    msg.Headers.Add("Raven-Index-Etag", queryOp.Header.IndexEtag.ToString());
                    msg.Headers.Add("Raven-Is-Stale", queryOp.Header.IsStable ? "true" : "false");
                    msg.Headers.Add("Raven-Index", queryOp.Header.Index);
                    msg.Headers.Add("Raven-Total-Results", queryOp.Header.TotalResults.ToString(CultureInfo.InvariantCulture));
                    msg.Headers.Add(
                        "Raven-Index-Timestamp", queryOp.Header.IndexTimestamp.ToString(Default.DateTimeFormatsToWrite, CultureInfo.InvariantCulture));

                }
                catch (Exception)
                {
                    accessor.Dispose();
                    throw;
                }

                return msg;
            }
		}

		public class StreamQueryContent : HttpContent
		{
			private readonly HttpRequestMessage req;
			private readonly DocumentDatabase.DatabaseQueryOperation queryOp;
			private readonly IStorageActionsAccessor accessor;
			private readonly Action<string> outputContentTypeSetter;

			public StreamQueryContent(HttpRequestMessage req, DocumentDatabase.DatabaseQueryOperation queryOp, IStorageActionsAccessor accessor,Action<string> contentTypeSetter)
			{
				this.req = req;
				this.queryOp = queryOp;
				this.accessor = accessor;
				outputContentTypeSetter = contentTypeSetter;
			}

			protected override Task SerializeToStreamAsync(Stream stream, TransportContext context)
			{
				using (var writer = GetOutputWriter(req, stream))
				{
					writer.WriteHeader();
					queryOp.Execute(writer.Write);
					outputContentTypeSetter(writer.ContentType);					
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
			string ContentType { get; }

			void WriteHeader();
			void Write(RavenJObject result);
		}

		private class ExcelOutputWriter : IOutputWriter
		{
			private const string CsvContentType = "text/csv";

			private readonly Stream stream;
			private StreamWriter writer;

			public ExcelOutputWriter( Stream stream)
			{
				this.stream = stream;
			}

			public string ContentType
			{
				get { return CsvContentType; }
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
					excludeParentPropertyNames: true).ToList();

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
			private const string JsonContentType = "application/json";
			private readonly Stream stream;
			private JsonWriter writer;

			public JsonOutputWriter(Stream stream)
			{
				this.stream = stream;
			}

			public string ContentType
			{
				get {  return JsonContentType; }
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