using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Principal;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Linq;
using Raven.Abstractions.Streaming;
using Raven.Abstractions.Util;
using Raven.Abstractions.Util.Encryptors;
using Raven.Database.Actions;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Database.Storage;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers
{
    public class StreamsController : ClusterAwareRavenDbApiController
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
            var transformer = GetQueryStringValue("transformer");
            var transformerParameters = ExtractTransformerParameters();

            var headers = CurrentOperationContext.Headers.Value;
            var user = CurrentOperationContext.User.Value;
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new PushStreamContent((stream, content, transportContext) =>
                    StreamToClient(stream, startsWith, start, pageSize, etag, matches, nextPageStart, skipAfter, transformer, transformerParameters, headers, user))
                {
                    Headers =
                    {
                        ContentType = new MediaTypeHeaderValue("application/json") { CharSet = "utf-8" }
                    }
                }
            };
        }

        private void StreamToClient(Stream stream, string startsWith, int start, int pageSize, Etag etag, string matches, int nextPageStart, string skipAfter, string transformer, Dictionary<string, RavenJToken> transformerParameters,
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
                    if (doc == null)
                    {
                        // we only have this heartbit when the streaming has gone on for a long time
                        // and we haven't send anything to the user in a while (because of filtering, skipping, etc).
                        writer.WriteRaw(Environment.NewLine);
                        writer.Flush();
                        return;
                    }
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
                            Database.Documents.GetDocuments(start, pageSize, etag, cts.Token, doc => { addDocument(doc); return true; }, transformer, transformerParameters);
                        }
                        else
                        {
                            var nextPageStartInternal = nextPageStart;

                            Database.Documents.GetDocumentsWithIdStartingWith(startsWith, matches, null, start, pageSize, cts.Token, ref nextPageStartInternal, addDocument, transformer, transformerParameters, skipAfter);

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
            var isHeadRequest = InnerRequest.Method == HttpMethods.Head;
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

        [HttpGet]
        [RavenRoute("streams/exploration")]
        [RavenRoute("databases/{databaseName}/streams/exploration")]
        public Task<HttpResponseMessage> Exploration()
        {
            var linq = GetQueryStringValue("linq");
            var collection = GetQueryStringValue("collection");
            int timeoutSeconds;
            if (int.TryParse(GetQueryStringValue("timeoutSeconds"), out timeoutSeconds) == false)
                timeoutSeconds = 60;
            int pageSize;
            if (int.TryParse(GetQueryStringValue("pageSize"), out pageSize) == false)
                pageSize = 100000;

            var hash = Encryptor.Current.Hash.Compute16(Encoding.UTF8.GetBytes(linq));
            var sourceHashed = MonoHttpUtility.UrlEncode(Convert.ToBase64String(hash));
            var transformerName = Constants.TemporaryTransformerPrefix + sourceHashed;

            var transformerDefinition = Database.IndexDefinitionStorage.GetTransformerDefinition(transformerName);
            if (transformerDefinition == null)
            {
                transformerDefinition = new TransformerDefinition
                {
                    Name = transformerName,
                    Temporary = true,
                    TransformResults = linq
                };
                Database.Transformers.PutTransform(transformerName, transformerDefinition);
            }

            var msg = GetEmptyMessage();

            using (var cts = new CancellationTokenSource())
            {
                var timeout = cts.TimeoutAfter(TimeSpan.FromSeconds(timeoutSeconds));
                var indexQuery = new IndexQuery
                {
                    PageSize = pageSize,
                    Start = 0,
                    Query = "Tag:" + collection,
                    ResultsTransformer = transformerName
                };

                var accessor = Database.TransactionalStorage.CreateAccessor(); //accessor will be disposed in the StreamQueryContent.SerializeToStreamAsync!

                try
                {
                    var queryOp = new QueryActions.DatabaseQueryOperation(Database, "Raven/DocumentsByEntityName", indexQuery, accessor, cts);
                    queryOp.Init();

                    msg.Content = new StreamQueryContent(InnerRequest, queryOp, accessor, timeout, mediaType => msg.Content.Headers.ContentType = new MediaTypeHeaderValue(mediaType) { CharSet = "utf-8" },
                        o =>
                        {
                            if (o.Count == 2 &&
                                o.ContainsKey(Constants.DocumentIdFieldName) &&
                                    o.ContainsKey(Constants.Metadata))
                            {
                                // this is the raw value out of the server, we don't want to get that
                                var doc = queryOp.DocRetriever.Load(o.Value<string>(Constants.DocumentIdFieldName));
                                var djo = doc as IDynamicJsonObject;
                                if (djo != null)
                                    return djo.Inner;
                            }
                            return o;
                        });
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
                catch (Exception)
                {
                    accessor.Dispose();
                    throw;
                }

                return new CompletedTask<HttpResponseMessage>(msg);
            }
        }

        [HttpPost]
        [RavenRoute("streams/query/{*id}")]
        [RavenRoute("databases/{databaseName}/streams/query/{*id}")]
        public async Task<HttpResponseMessage> SteamQueryPost(string id)
        {
            var postedQuery = await ReadStringAsync().ConfigureAwait(false);

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
            private readonly Func<RavenJObject, RavenJObject> modifyDocument;

            [CLSCompliant(false)]
            public StreamQueryContent(HttpRequestMessage req, QueryActions.DatabaseQueryOperation queryOp, IStorageActionsAccessor accessor,
                CancellationTimeout timeout,
                Action<string> contentTypeSetter,
                Func<RavenJObject,RavenJObject> modifyDocument = null)
            {
                headers = CurrentOperationContext.Headers.Value;
                user = CurrentOperationContext.User.Value;
                this.req = req;
                this.queryOp = queryOp;
                this.accessor = accessor;
                _timeout = timeout;
                outputContentTypeSetter = contentTypeSetter;
                this.modifyDocument = modifyDocument;
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
                            if (modifyDocument != null)
                                o = modifyDocument(o);
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
            return useExcelFormat ? (IOutputWriter)new ExcelOutputWriter(stream, GetQueryStringValues(req, "column")) : new JsonOutputWriter(stream);
        }

        private static bool IsCsvDownloadRequest(HttpRequestMessage req)
        {
            return "true".Equals(GetQueryStringValue(req, "download"), StringComparison.InvariantCultureIgnoreCase)
                && "excel".Equals(GetQueryStringValue(req, "format"), StringComparison.InvariantCultureIgnoreCase);
        }
    }
}
