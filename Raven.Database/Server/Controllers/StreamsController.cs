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
        public async Task<HttpResponseMessage> SteamQueryGet(string id)
        {
            var cts = new CancellationTokenSource();
            var timeout = cts.TimeoutAfter(DatabasesLandlord.SystemConfiguration.DatabaseOperationTimeout);
            var msg = GetEmptyMessage();

            var index = id;
            var query = GetIndexQuery(int.MaxValue);
            if (string.IsNullOrEmpty(GetQueryStringValue("pageSize"))) query.PageSize = int.MaxValue;
            var isHeadRequest = InnerRequest.Method == HttpMethods.Head;
            if (isHeadRequest) query.PageSize = 0;

            StreamQueryContent streamQueryContent = null;
            try
            {
                var parameters = new StreamQueryContent.InitParameters
                {
                    Req = InnerRequest,
                    Database = Database,
                    IndexName = index,
                    Cts = cts,
                    Timeout = timeout,
                    ContentTypeSetter = mediaType => msg.Content.Headers.ContentType = new MediaTypeHeaderValue(mediaType) { CharSet = "utf-8" },
                    Query = query,


                };
                streamQueryContent = new StreamQueryContent(parameters);
                msg.Content = streamQueryContent;
                var header = await Task.Run(
                    async ()=>
                        await streamQueryContent.HeaderReady.ConfigureAwait(false),
                    cts.Token)
                    .ConfigureAwait(false);
                
                msg.Headers.Add("Raven-Result-Etag", header.ResultEtag.ToString());
                msg.Headers.Add("Raven-Index-Etag", header.IndexEtag.ToString());
                msg.Headers.Add("Raven-Is-Stale", header.IsStale ? "true" : "false");
                msg.Headers.Add("Raven-Index", header.Index);
                msg.Headers.Add("Raven-Total-Results", header.TotalResults.ToString(CultureInfo.InvariantCulture));
                msg.Headers.Add("Raven-Index-Timestamp", header.IndexTimestamp.GetDefaultRavenFormat());

                if (IsCsvDownloadRequest(InnerRequest))
                {
                    msg.Content.Headers.Add("Content-Disposition", "attachment; filename=export.csv");
                }
            }
            catch (OperationCanceledException e)
            {
                streamQueryContent?.Dispose();
                throw new TimeoutException(string.Format("The query did not produce results in {0}", DatabasesLandlord.SystemConfiguration.DatabaseOperationTimeout), e);
            }
            catch (Exception)
            {
                streamQueryContent?.Dispose();
                throw;
            }

            return msg;
        }

        [HttpGet]
        [RavenRoute("streams/exploration")]
        [RavenRoute("databases/{databaseName}/streams/exploration")]
        public async Task<HttpResponseMessage> Exploration(string collection)
        {
            var linq = GetQueryStringValue("linq");
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

            var cts = new CancellationTokenSource();
            var timeout = cts.TimeoutAfter(TimeSpan.FromSeconds(timeoutSeconds));
            var indexQuery = new IndexQuery
            {
                PageSize = pageSize,
                Start = 0,
                Query = "Tag:" + collection,
                ResultsTransformer = transformerName
            };

            StreamQueryContent streamQueryContent = null;

            try
            {
                var parameters = new StreamQueryContent.InitParameters
                {
                    Req = InnerRequest,
                    Database = Database,
                    Cts = cts,
                    Timeout = timeout,
                    ContentTypeSetter = mediaType => msg.Content.Headers.ContentType = new MediaTypeHeaderValue(mediaType) { CharSet = "utf-8" },
                    Query = indexQuery,                        
                };
                streamQueryContent = new StreamQueryContent(parameters);
                msg.Content = streamQueryContent;

                var header = await Task.Run(async ()=> await streamQueryContent.HeaderReady.ConfigureAwait(false), cts.Token).ConfigureAwait(false);
                //This is just a callback, it should be invoked in the same thread as the query was invoked at.
                streamQueryContent.ModifyDocument = (queryOp, o) =>
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
                    };
                msg.Headers.Add("Raven-Result-Etag", header.ResultEtag.ToString());
                msg.Headers.Add("Raven-Index-Etag", header.IndexEtag.ToString());
                msg.Headers.Add("Raven-Is-Stale", header.IsStale ? "true" : "false");
                msg.Headers.Add("Raven-Index", header.Index);
                msg.Headers.Add("Raven-Total-Results", header.TotalResults.ToString(CultureInfo.InvariantCulture));
                msg.Headers.Add("Raven-Index-Timestamp", header.IndexTimestamp.GetDefaultRavenFormat());

                if (IsCsvDownloadRequest(InnerRequest))
                {
                    msg.Content.Headers.Add("Content-Disposition", "attachment; filename=export.csv");
                }
            }
            catch (Exception)
            {
                streamQueryContent?.Dispose();
                throw;
            }

            return msg;
        }

        [HttpPost]
        [RavenRoute("streams/query/{*id}")]
        [RavenRoute("databases/{databaseName}/streams/query/{*id}")]
        public async Task<HttpResponseMessage> SteamQueryPost(string id)
        {
            var postedQuery = await ReadStringAsync().ConfigureAwait(false);

            SetPostRequestQuery(postedQuery);

            return await SteamQueryGet(id).ConfigureAwait(false);
        }

        public class StreamQueryContent : HttpContent
        {
            public QueryActions.DatabaseQueryOperation QueryOp { get; private set; }
            private readonly HttpRequestMessage req;
            private readonly CancellationTokenSource _cts;
            private readonly TaskCompletionSource<QueryHeaderInformation> _headerReady = new TaskCompletionSource<QueryHeaderInformation>();
            private readonly TaskCompletionSource<Stream> _streamReady = new TaskCompletionSource<Stream>();
            public Task<QueryHeaderInformation> HeaderReady => _headerReady.Task;
            private readonly Task _streamTask;

            public void SetStream(Stream s)
            {
                _streamReady.TrySetResult(s);
            }

            private readonly CancellationTimeout _timeout;
            private readonly Action<string> outputContentTypeSetter;
            private Lazy<NameValueCollection> headers;
            private IPrincipal user;
            public Func<QueryActions.DatabaseQueryOperation, RavenJObject, RavenJObject> ModifyDocument;

            public class InitParameters
            {
                public HttpRequestMessage Req { get; set; }
                public DocumentDatabase Database { get; set; }
                public string IndexName { get; set; } = "Raven/DocumentsByEntityName";
                public IndexQuery Query { get; set; }
                public CancellationTokenSource Cts { get; set; }
                public CancellationTimeout Timeout { get; set; }
                public Action<string> ContentTypeSetter { get; set; }
                public Func<QueryActions.DatabaseQueryOperation,RavenJObject, RavenJObject> ModifyDocument { get; set; }
            }

            [CLSCompliant(false)]
            public StreamQueryContent(InitParameters parameters)
            {
                headers = CurrentOperationContext.Headers.Value;
                user = CurrentOperationContext.User.Value;
                req = parameters.Req;
                _cts = parameters.Cts;
                _streamTask = Task.Run(() => { ActuallyStreamResults(parameters); }, parameters.Cts.Token);
                _timeout = parameters.Timeout;
                outputContentTypeSetter = parameters.ContentTypeSetter;
                ModifyDocument = parameters.ModifyDocument;
            }

            private void ActuallyStreamResults(InitParameters parameters)
            {
                IStorageActionsAccessor accessor = null;
                try
                {

                    accessor = parameters.Database.TransactionalStorage.CreateAccessor();
                    QueryOp = new QueryActions.DatabaseQueryOperation(parameters.Database, parameters.IndexName, parameters.Query, accessor, parameters.Cts);
                    QueryOp.Init();
                    _headerReady.TrySetResult(QueryOp.Header);
                    _streamReady.Task.Wait(parameters.Cts.Token);
                    SerializeToStream(_streamReady.Task.Result, QueryOp.Header.TotalResults);
                } 
                catch( Exception e)
                {
                    _headerReady.TrySetException(e);
                }
                finally
                {
                    accessor?.Dispose();
                }
            }

            protected override Task SerializeToStreamAsync(Stream stream, TransportContext context)
            {
                _streamReady.TrySetResult(stream);
                return _streamTask;
            }

            private void SerializeToStream(Stream stream, long totalResults)
            {
                var old = CurrentOperationContext.Headers.Value;
                var oldUser = CurrentOperationContext.User.Value;
                try
                {
                    CurrentOperationContext.User.Value = user;
                    CurrentOperationContext.Headers.Value = headers;
                    var bufferSize = totalResults > 1024 ? 1024 * 64 : 1024 * 8;
                    using(_cts)
                    using (var bufferedStream = new BufferedStream(stream, bufferSize))
                    using (QueryOp)
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
                            QueryOp.Execute(o =>
                            {
                                _timeout.Delay();
                                if (ModifyDocument != null)
                                    o = ModifyDocument(QueryOp, o);
                                writer.Write(o);
                            });
                        }
                        catch (Exception e)
                        {
                            writer.WriteError(e);
                        }
                    }
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
            return useExcelFormat ? 
                (IOutputWriter)new ExcelOutputWriter(stream, GetQueryStringValues(req, "column")) : 
                new JsonOutputWriter(stream);
        }

        private static bool IsCsvDownloadRequest(HttpRequestMessage req)
        {
            return "true".Equals(GetQueryStringValue(req, "download"), StringComparison.InvariantCultureIgnoreCase)
                && "excel".Equals(GetQueryStringValue(req, "format"), StringComparison.InvariantCultureIgnoreCase);
        }
    }
}
