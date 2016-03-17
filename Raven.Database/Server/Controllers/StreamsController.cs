using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Database.Actions;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Database.Storage;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;
using System.Linq;
using System.Security.Principal;

namespace Raven.Database.Server.Controllers
{
    public class StreamsController : RavenDbApiController
    {
        [HttpHead]
        [RavenRoute("streams/docs")]
        [RavenRoute("databases/{databaseName}/streams/docs")]
        public HttpResponseMessage StreamDocsHead()
        {
            return GetEmptyMessage();
        }

        [HttpGet]
        [RavenRoute("streams/docs")]
        [RavenRoute("databases/{databaseName}/streams/docs")]
        public HttpResponseMessage StreamDocsGet()
        {
            var start = GetStart();
            var etag = GetEtagFromQueryString();
            var startsWith = GetQueryStringValue("startsWith");
            var pageSize = GetPageSize(int.MaxValue);
            var matches = GetQueryStringValue("matches");
            var nextPageStart = GetNextPageStart();
            if (string.IsNullOrEmpty(GetQueryStringValue("pageSize")))
                pageSize = int.MaxValue;

            var skipAfter = GetQueryStringValue("skipAfter");

            
            var headers = CurrentOperationContext.Headers.Value;
            var user = CurrentOperationContext.User.Value;
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new PushStreamContent((stream, content, transportContext) =>
                    StreamToClient(stream, startsWith, start, pageSize, etag, matches, nextPageStart, skipAfter, headers, user))
                {
                    Headers =
                    {
                        ContentType = new MediaTypeHeaderValue("application/json") { CharSet = "utf-8" }
                    }
                }
            };
        }

        private void StreamToClient(Stream stream, string startsWith, int start, int pageSize, Etag etag, string matches, int nextPageStart, string skipAfter, 
            Lazy<NameValueCollection> headers, IPrincipal user)
        {
            var old = CurrentOperationContext.Headers.Value;
            var oldUser = CurrentOperationContext.User.Value;
            try
            {
                CurrentOperationContext.Headers.Value = headers;
                CurrentOperationContext.User.Value = user;


                var bufferStream = new BufferedStream(stream, 1024 * 64);
                using (var cts = new CancellationTokenSource())
                using (var timeout = cts.TimeoutAfter(DatabasesLandlord.SystemConfiguration.DatabaseOperationTimeout))
                using (var writer = new JsonTextWriter(new StreamWriter(bufferStream)))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Results");
                    writer.WriteStartArray();

                    Action<JsonDocument> addDocument = doc =>
                    {
                        timeout.Delay();
                        doc.ToJson().WriteTo(writer);
                        writer.WriteRaw(Environment.NewLine);
                    };

                    Database.TransactionalStorage.Batch(accessor =>
                    {
                        // we may be sending a LOT of documents to the user, and most 
                        // of them aren't going to be relevant for other ops, so we are going to skip
                        // the cache for that, to avoid filling it up very quickly
                        using (DocumentCacher.SkipSetAndGetDocumentsInDocumentCache())
                        {
                            if (string.IsNullOrEmpty(startsWith))
                            {
                                Database.Documents.GetDocuments(start, pageSize, etag, cts.Token, addDocument);
                            }
                            else
                            {
                                var nextPageStartInternal = nextPageStart;

                                Database.Documents.GetDocumentsWithIdStartingWith(startsWith, matches, null, start, pageSize, cts.Token, ref nextPageStartInternal, addDocument, skipAfter: skipAfter);

                                nextPageStart = nextPageStartInternal;
                            }
                        }
                    });

                    writer.WriteEndArray();
                    writer.WritePropertyName("NextPageStart");
                    writer.WriteValue(nextPageStart);
                    writer.WriteEndObject();
                    writer.Flush();
                    bufferStream.Flush();
                }
            }
            finally
            {
                CurrentOperationContext.Headers.Value = old;
                CurrentOperationContext.User.Value = oldUser;
            }
        }

        [HttpHead]
        [RavenRoute("streams/query/{*id}")]
        [RavenRoute("databases/{databaseName}/streams/query/{*id}")]
        public HttpResponseMessage SteamQueryHead(string id)
        {
            return GetEmptyMessage();
        }

        [HttpGet]
        [RavenRoute("streams/query/{*id}")]
        [RavenRoute("databases/{databaseName}/streams/query/{*id}")]
        public HttpResponseMessage SteamQueryGet(string id)
        {
            var cts = new CancellationTokenSource();
            var timeout = cts.TimeoutAfter(DatabasesLandlord.SystemConfiguration.DatabaseOperationTimeout);
            var msg = GetEmptyMessage();

            var index = id;
            var query = GetIndexQuery(int.MaxValue);
            if (string.IsNullOrEmpty(GetQueryStringValue("pageSize"))) query.PageSize = int.MaxValue;
            var isHeadRequest = InnerRequest.Method == HttpMethod.Head;
            if (isHeadRequest) query.PageSize = 0;

            var accessor = Database.TransactionalStorage.CreateAccessor(); //accessor will be disposed in the StreamQueryContent.SerializeToStreamAsync!

            try
            {
                var queryOp = new QueryActions.DatabaseQueryOperation(Database, index, query, accessor, cts);
                queryOp.Init();

                msg.Content = new StreamQueryContent(InnerRequest, queryOp, accessor, timeout, mediaType => msg.Content.Headers.ContentType = new MediaTypeHeaderValue(mediaType) {CharSet = "utf-8"});
                msg.Headers.Add("Raven-Result-Etag", queryOp.Header.ResultEtag.ToString());
                msg.Headers.Add("Raven-Index-Etag", queryOp.Header.IndexEtag.ToString());
                msg.Headers.Add("Raven-Is-Stale", queryOp.Header.IsStale ? "true" : "false");
                msg.Headers.Add("Raven-Index", queryOp.Header.Index);
                msg.Headers.Add("Raven-Total-Results", queryOp.Header.TotalResults.ToString(CultureInfo.InvariantCulture));
                msg.Headers.Add("Raven-Index-Timestamp", queryOp.Header.IndexTimestamp.GetDefaultRavenFormat());

                if (IsCsvDownloadRequest(InnerRequest))
                {
                    msg.Content.Headers.Add("Content-Disposition", "attachment; filename=export.csv");
                }
            }
            catch (OperationCanceledException e)
            {
                accessor.Dispose();
                throw new TimeoutException(string.Format("The query did not produce results in {0}", DatabasesLandlord.SystemConfiguration.DatabaseOperationTimeout), e);
            }
            catch (Exception)
            {
                accessor.Dispose();
                throw;
            }

            return msg;
        }

        [HttpPost]
        [RavenRoute("streams/query/{*id}")]
        [RavenRoute("databases/{databaseName}/streams/query/{*id}")]
        public async Task<HttpResponseMessage> SteamQueryPost(string id)
        {
            var postedQuery = await ReadStringAsync();

            SetPostRequestQuery(postedQuery);

            return SteamQueryGet(id);
        }

        public class StreamQueryContent : HttpContent
        {
            private readonly HttpRequestMessage req;
            private readonly QueryActions.DatabaseQueryOperation queryOp;
            private readonly IStorageActionsAccessor accessor;
            private readonly CancellationTimeout _timeout;
            private readonly Action<string> outputContentTypeSetter;
            private Lazy<NameValueCollection> headers;
            private IPrincipal user;

            [CLSCompliant(false)]
            public StreamQueryContent(HttpRequestMessage req, QueryActions.DatabaseQueryOperation queryOp, IStorageActionsAccessor accessor, CancellationTimeout timeout, Action<string> contentTypeSetter)
            {
                headers = CurrentOperationContext.Headers.Value;
                user = CurrentOperationContext.User.Value;
                this.req = req;
                this.queryOp = queryOp;
                this.accessor = accessor;
                _timeout = timeout;
                outputContentTypeSetter = contentTypeSetter;
            }

            protected override Task SerializeToStreamAsync(Stream stream, TransportContext context)
            {
                var old = CurrentOperationContext.Headers.Value;
                var oldUser = CurrentOperationContext.User.Value;
                try
                {
                    CurrentOperationContext.User.Value = user;
                    CurrentOperationContext.Headers.Value = headers;
                    var bufferSize = queryOp.Header.TotalResults > 1024 ? 1024 * 64 : 1024 * 8;
                    using (var bufferedStream = new BufferedStream(stream, bufferSize))
                    using (queryOp)
                    using (accessor)
                    using (_timeout)
                    using (var writer = GetOutputWriter(req, bufferedStream))
                    // we may be sending a LOT of documents to the user, and most 
                    // of them aren't going to be relevant for other ops, so we are going to skip
                    // the cache for that, to avoid filling it up very quickly
                    using (DocumentCacher.SkipSetAndGetDocumentsInDocumentCache())
                    {
                        outputContentTypeSetter(writer.ContentType);

                        writer.WriteHeader();
                        try
                        {
                            queryOp.Execute(o =>
                            {
                                _timeout.Delay();
                                writer.Write(o);
                            });
                        }
                        catch (Exception e)
                        {
                            writer.WriteError(e);
                        }
                    }
                    return Task.FromResult(true);
                }
                finally
                {
                    CurrentOperationContext.Headers.Value = old;
                    CurrentOperationContext.User.Value = oldUser;
                }
            }

            protected override bool TryComputeLength(out long length)
            {
                length = -1;
                return false;
            }
        }

        private static IOutputWriter GetOutputWriter(HttpRequestMessage req, Stream stream)
        {
            var useExcelFormat = "excel".Equals(GetQueryStringValue(req, "format"), StringComparison.InvariantCultureIgnoreCase);
            return useExcelFormat ? (IOutputWriter)new ExcelOutputWriter(stream) : new JsonOutputWriter(stream);
        }

        private static Boolean IsCsvDownloadRequest(HttpRequestMessage req)
        {
            return "true".Equals(GetQueryStringValue(req, "download"), StringComparison.InvariantCultureIgnoreCase)
                && "excel".Equals(GetQueryStringValue(req, "format"), StringComparison.InvariantCultureIgnoreCase);
        }


        public interface IOutputWriter : IDisposable
        {
            string ContentType { get; }

            void WriteHeader();
            void Write(RavenJObject result);
            void WriteError(Exception exception);
        }

        private class ExcelOutputWriter : IOutputWriter
        {
            private const string CsvContentType = "text/csv";

            private readonly Stream stream;
            private StreamWriter writer;
            private bool doIncludeId;

            public ExcelOutputWriter(Stream stream)
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
                stream.Flush();
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
                    GetPropertiesAndWriteCsvHeader(result, out doIncludeId);
                    Debug.Assert(properties != null);
                }

                if (doIncludeId)
                {
                    RavenJToken token;
                    if (result.TryGetValue("@metadata", out token))
                    {
                        var metadata = token as RavenJObject;
                        if (metadata != null)
                        {
                            if (metadata.TryGetValue("@id", out token))
                            {
                                OutputCsvValue(token.Value<string>());
                            }
                            writer.Write(',');
                        }
                    }
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

            public void WriteError(Exception exception)
            {
                writer.WriteLine();
                writer.WriteLine();
                writer.WriteLine(exception.ToString());
            }

            private void GetPropertiesAndWriteCsvHeader(RavenJObject result, out bool includeId)
            {
                includeId = false;
                properties = DocumentHelpers.GetPropertiesFromJObject(result,
                    parentPropertyPath: "",
                    includeNestedProperties: true,
                    includeMetadata: false,
                    excludeParentPropertyNames: true).ToList();

                RavenJToken token;
                if (result.TryGetValue("@metadata", out token))
                {
                    var metadata = token as RavenJObject;
                    if (metadata != null)
                    {
                        if (metadata.TryGetValue("@id", out token))
                        {
                            OutputCsvValue("@id");
                            writer.Write(',');

                            includeId = true;
                        }
                    }
                }

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
            private bool closedArray = false;

            public JsonOutputWriter(Stream stream)
            {
                this.stream = stream;
            }

            public string ContentType
            {
                get { return JsonContentType; }
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
                if (closedArray == false)
                    writer.WriteEndArray();
                writer.WriteEndObject();

                writer.Flush();
                stream.Flush();
                writer.Close();
            }

            public void Write(RavenJObject result)
            {
                result.WriteTo(writer, Default.Converters);
                writer.WriteRaw(Environment.NewLine);
            }

            public void WriteError(Exception exception)
            {
                closedArray = true;
                writer.WriteEndArray();
                writer.WritePropertyName("Error");
                writer.WriteValue(exception.ToString());
            }
        }
    }
}
