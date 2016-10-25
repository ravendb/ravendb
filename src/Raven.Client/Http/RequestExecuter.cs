using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using Raven.Abstractions.Connection;
using Raven.Client.Connection;
using Raven.Client.Documents.Commands;
using Sparrow.Json;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Util;
using Raven.Client.Exceptions;
using Sparrow.Logging;

namespace Raven.Client.Http
{
    public class RequestExecuter : IDisposable
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<RequestExecuter>("Client");

        public readonly JsonContextPool ContextPool;

        public class AggresiveCacheOptions
        {
            public TimeSpan Duration;
        }

        private readonly ApiKeyAuthenticator _authenticator = new ApiKeyAuthenticator();

        public readonly ThreadLocal<AggresiveCacheOptions> AggressiveCaching = new ThreadLocal<AggresiveCacheOptions>();

        private readonly HttpCache _cache = new HttpCache();

        private readonly HttpClient _httpClient;

        private Topology _topology;
        private readonly Timer _updateTopologyTimer;
        private bool _firstTimeTryLoadFromTopologyCache = true;

        private Timer _updateCurrentTokenTimer;
        private readonly Timer _updateFailingNodesStatus;

        public RequestExecuter(string url, string databaseName, string apiKey)
        {
            _topology = new Topology
            {
                LeaderNode = new ServerNode
                {
                    Url = url,
                    Database = databaseName,
                    ApiKey = apiKey,
                },
                ReadBehavior = ReadBehavior.LeaderOnly,
                WriteBehavior = WriteBehavior.LeaderOnly,
                Etag = int.MinValue,
            };

            var handler = new HttpClientHandler();
            _httpClient = new HttpClient(handler);

            ContextPool = new JsonContextPool();

            _updateTopologyTimer = new Timer(UpdateTopologyCallback, null, 0, Timeout.Infinite);
            _updateFailingNodesStatus = new Timer(UpdateFailingNodesStatusCallback, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
        }

        private void UpdateTopologyCallback(object _)
        {
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
            UpdateTopology();
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
        }

        private async Task UpdateTopology()
        {
            JsonOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var node = _topology.LeaderNode;

                var serverHash = ServerHash.GetServerHash(node.Url, node.Database);

                if (_firstTimeTryLoadFromTopologyCache)
                {
                    _firstTimeTryLoadFromTopologyCache = false;

                    var cachedTopology = TopologyLocalCache.TryLoadTopologyFromLocalCache(serverHash, context);
                    if (cachedTopology != null && cachedTopology.Etag > 0)
                    {
                        _topology = cachedTopology;
                        // we have cached topology, but we need to verify it is up to date, we'll check in 
                        // 1 second, and let the rest of the system start
                        _updateTopologyTimer.Change(TimeSpan.FromSeconds(1), Timeout.InfiniteTimeSpan);
                        return;
                    }
                }

                var command = new GetTopologyCommand();
                try
                {
                    await ExecuteAsync(new ChoosenNode {Node = node}, context, command);
                    if (_topology.Etag != command.Result.Etag)
                    {
                        _topology = command.Result;
                        TopologyLocalCache.TrySavingTopologyToLocalCache(serverHash, _topology, context);
                    }
                }
                catch (Exception ex)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info("Failed to update topology", ex);
                }
                finally
                {
                    _updateTopologyTimer.Change(TimeSpan.FromMinutes(5), Timeout.InfiniteTimeSpan);
                }
            }
        }

        public void Execute<TResult>(RavenCommand<TResult> command, JsonOperationContext context)
        {
            AsyncHelpers.RunSync(() => ExecuteAsync(command, context));
        }

        public async Task ExecuteAsync<TResult>(RavenCommand<TResult> command, JsonOperationContext context)
        {
            var choosenNode = ChooseNodeForRequest(command);

            await ExecuteAsync(choosenNode, context, command);
        }

        public async Task ExecuteAsync<TResult>(ChoosenNode choosenNode, JsonOperationContext context, RavenCommand<TResult> command)
        {
            string url;
            var request = CreateRequest(choosenNode.Node, command, out url);

            long cachedEtag;
            BlittableJsonReaderObject cachedValue;
            HttpCache.ReleaseCacheItem cachedItem;
            using (cachedItem = GetFromCache(context, command, request, url, out cachedEtag, out cachedValue))
            {
                if (cachedEtag != 0)
                {
                    var aggresiveCacheOptions = AggressiveCaching.Value;
                    if (aggresiveCacheOptions != null && cachedItem.Age < aggresiveCacheOptions.Duration)
                    {
                        command.SetResponse(cachedValue);
                        return;
                    }

                    request.Headers.IfNoneMatch.Add(new EntityTagHeaderValue(cachedEtag.ToString()));
                }

                var sp = Stopwatch.StartNew();
                HttpResponseMessage response;
                try
                {
                    response = await _httpClient.SendAsync(request).ConfigureAwait(false);
                    sp.Stop();
                }
                catch (HttpRequestException e) // server down, network down
                {
                    sp.Stop();
                    await HandleServerDown(choosenNode, context, command, e);
                    return;
                }
                finally
                {
                    var requestTimeInMilliseconds = sp.ElapsedMilliseconds;
                    choosenNode.Node.UpdateRequestTime(requestTimeInMilliseconds);
                    if (choosenNode.SkippedNodes != null)
                    {
                        foreach (var skippedNode in choosenNode.SkippedNodes)
                        {
                            skippedNode.DecreaseRate(requestTimeInMilliseconds);
                        }
                    }
                }

                using (response)
                {
                    if (response.StatusCode == HttpStatusCode.NotModified)
                    {
                        cachedItem.NotModified();
                        command.SetResponse(cachedValue);
                        return;
                    }
                    if (response.IsSuccessStatusCode == false)
                    {
                        if (await HandleUnsuccessfulResponse(choosenNode, context, command, response, url))
                            return;
                    }

                    using (var stream = await response.Content.ReadAsStreamAsync())
                    {
                        // we intentionally don't dispose the reader here, we'll be using it
                        // in the command, any associated memory will be released on context reset
                        var blittableJsonReaderObject = await context.ReadForMemoryAsync(stream, "PutResult");
                        if (response.Headers.ETag != null)
                        {
                            long etag;
                            if (long.TryParse(response.Headers.ETag.Tag, out etag))
                            {
                                _cache.Set(url, etag, blittableJsonReaderObject);
                            }
                        }
                        command.SetResponse(blittableJsonReaderObject);
                    }
                }
            }
        }

        private HttpCache.ReleaseCacheItem GetFromCache<TResult>(JsonOperationContext context, RavenCommand<TResult> command, HttpRequestMessage request, string url, out long cachedEtag, out BlittableJsonReaderObject cachedValue)
        {
            if (command.IsReadRequest)
            {
                if (request.Method != HttpMethod.Get)
                    url = request.Method + "-" + url;
                return _cache.Get(context, url, out cachedEtag, out cachedValue);
            }

            cachedEtag = 0;
            cachedValue = null;
            return new HttpCache.ReleaseCacheItem();
        }

        private static HttpRequestMessage CreateRequest<TResult>(ServerNode node, RavenCommand<TResult> command, out string url)
        {
            var request = command.CreateRequest(node, out url);
            url = $"{node.Url}/databases/{node.Database}/{url}";
            request.RequestUri = new Uri(url);

            if (node.CurrentToken != null)
                request.Headers.Add("Raven-Authorization", node.CurrentToken);

            return request;
        }

        private async Task<bool> HandleUnsuccessfulResponse<TResult>(ChoosenNode choosenNode, JsonOperationContext context, RavenCommand<TResult> command,
            HttpResponseMessage response, string url)
        {
            switch (response.StatusCode)
            {
                case HttpStatusCode.NotFound:
                    command.SetResponse(null);
                    return true;
                case HttpStatusCode.Unauthorized:
                case HttpStatusCode.PreconditionFailed:
                    if (string.IsNullOrEmpty(choosenNode.Node.ApiKey))
                        throw new UnauthorizedAccessException(
                            $"Got unauthorized response exception for {url}. Please specify an API Key.");
                    if (++command.AuthenticationRetries > 1)
                        throw new UnauthorizedAccessException(
                            $"Got unauthorized response exception for {url} after trying to authenticate using ApiKey.");

                    var oauthSource = response.Headers.GetFirstValue("OAuth-Source");

#if DEBUG && FIDDLER
// Make sure to avoid a cross DNS security issue, when running with Fiddler
                if (string.IsNullOrEmpty(oauthSource) == false)
                    oauthSource = oauthSource.Replace("localhost:", "localhost.fiddler:");
#endif

                    await HandleUnauthorized(oauthSource, choosenNode.Node, context).ConfigureAwait(false);
                    await ExecuteAsync(choosenNode, context, command).ConfigureAwait(false);
                    return true;
                case HttpStatusCode.Forbidden:
                    throw new UnauthorizedAccessException(
                        $"Forbidan access to {url}. Make sure you're using the correct ApiKey.");
                case HttpStatusCode.BadGateway:
                case HttpStatusCode.ServiceUnavailable:
                    await HandleServerDown(choosenNode, context, command, null);
                    break;
                case HttpStatusCode.Conflict:
                    // TODO: Conflict resolution
                    //TODO - Efrat - current implementation is temporary
                    using (var stream = await response.Content.ReadAsStreamAsync())
                    {
                        var blittableJsonReaderObject = await context.ReadForMemoryAsync(stream, "PutResult");
                        object o;
                        blittableJsonReaderObject.TryGetMember("Type", out o);
                        object m;
                        blittableJsonReaderObject.TryGetMember("Message", out m);
                        if (o.ToString() == "Voron.Exceptions.ConcurrencyException")
                            throw new ConcurrencyException();
                        break;
                    }
                default:
                    await ThrowServerError(context, response);
                    break;
            }
            return false;
        }

        public static async Task<MemoryStream> ReadAsStreamUncompressedAsync(HttpResponseMessage response)
        {
            using (var serverStream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false))
            {
                var stream = serverStream;
                var encoding = response.Content.Headers.ContentEncoding.FirstOrDefault();
                if (encoding != null && encoding.Contains("gzip"))
                    stream = new GZipStream(stream, CompressionMode.Decompress);
                else if (encoding != null && encoding.Contains("deflate"))
                    stream = new DeflateStream(stream, CompressionMode.Decompress);
                var ms = new MemoryStream();

                var buffer = new byte[4096];
                int read;
                while ((read = stream.Read(buffer, 0, buffer.Length)) != 0)
                {
                    ms.Write(buffer, 0, read);
                }
                stream.Dispose();
                ms.Position = 0;
                return ms;
            }
        }

        private static async Task ThrowServerError(JsonOperationContext context, HttpResponseMessage response)
        {
            using (var stream = await ReadAsStreamUncompressedAsync(response))
            {
                BlittableJsonReaderObject blittableJsonReaderObject;
                try
                {
                    blittableJsonReaderObject = await context.ReadForMemoryAsync(stream, "ErrorResponse");
                }
                catch (Exception e)
                {
                    stream.Position = 0;
                    throw new InvalidOperationException(
                        $"Cannot parse the {response.StatusCode} response: {new StreamReader(stream).ReadToEnd()}", e);
                }
                stream.Position = 0;
                using (blittableJsonReaderObject)
                {
                    string error;
                    if (blittableJsonReaderObject.TryGet("Error", out error) == false)
                        throw new InvalidOperationException(
                            $"Doesn't know how to handle error: {response.StatusCode}, response: {new StreamReader(stream).ReadToEnd()}");

                    if (response.StatusCode == HttpStatusCode.BadRequest)
                        throw new BadRequestException(error + ". Response: " + blittableJsonReaderObject);

                    string indexDefinitionProperty;
                    if (blittableJsonReaderObject.TryGet(nameof(IndexCompilationException.IndexDefinitionProperty),
                        out indexDefinitionProperty))
                    {
                        var indexCompilationException = new IndexCompilationException(error);
                        blittableJsonReaderObject.TryGet(nameof(IndexCompilationException.IndexDefinitionProperty),
                            out indexCompilationException.IndexDefinitionProperty);
                        blittableJsonReaderObject.TryGet(nameof(IndexCompilationException.ProblematicText),
                            out indexCompilationException.ProblematicText);
                        throw indexCompilationException;
                    }

                    throw new InternalServerErrorException(error + ". Response: " + blittableJsonReaderObject);
                }
            }
        }

        private async Task HandleServerDown<TResult>(ChoosenNode choosenNode, JsonOperationContext context, RavenCommand<TResult> command,
            HttpRequestException e)
        {
            if (command.FailedNodes == null)
                command.FailedNodes = new HashSet<ServerNode>();

            choosenNode.Node.IsFailed = true;
            command.FailedNodes.Add(choosenNode.Node);

            var failoverNode = ChooseNodeForRequest(command, e);
            await ExecuteAsync(failoverNode, context, command);
        }

        public class ChoosenNode
        {
            public ServerNode Node;
            public List<ServerNode> SkippedNodes;
        }

        private ChoosenNode ChooseNodeForRequest<T>(RavenCommand<T> command, HttpRequestException exception = null)
        {
            var topology = _topology;

            var leaderNode = topology.LeaderNode;

            if (command.IsReadRequest)
            {
                if (topology.ReadBehavior == ReadBehavior.LeaderOnly)
                {
                    if (command.IsFailedWithNode(leaderNode) == false)
                        return new ChoosenNode {Node = leaderNode};
                    throw new HttpRequestException("Leader not was failed to make this request. The current ReadBehavior is set to Leader to we won't failover to a differnt node.", exception);
                }

                if (topology.ReadBehavior == ReadBehavior.RoundRobin)
                {
                    if (leaderNode.IsFailed == false && command.IsFailedWithNode(leaderNode) == false)
                        return new ChoosenNode {Node = leaderNode};

                    // TODO: Should we choose nodes here by rate value as for SLA?
                    var choosenNode = new ChoosenNode {SkippedNodes = new List<ServerNode>()};
                    foreach (var node in topology.Nodes)
                    {
                        if (node.IsFailed == false && command.IsFailedWithNode(node) == false)
                        {
                            choosenNode.Node = node;
                            return choosenNode;
                        }

                        choosenNode.SkippedNodes.Add(node);
                    }

                    throw new HttpRequestException("Tried all nodes in the cluster but failed getting a response", exception);
                }

                if (topology.ReadBehavior == ReadBehavior.LeaderWithFailoverWhenRequestTimeSlaThresholdIsReached)
                {
                    if (leaderNode.IsFailed == false && command.IsFailedWithNode(leaderNode) == false && leaderNode.IsRateSurpassed(topology.SLA.RequestTimeThresholdInMilliseconds))
                        return new ChoosenNode {Node = leaderNode};

                    var nodesWithLeader = topology.Nodes
                        .Union(new[] { leaderNode })
                        .OrderBy(node => node.Rate())
                        .ToList();
                    var fastestNode = nodesWithLeader.FirstOrDefault(node => node.IsFailed == false && command.IsFailedWithNode(node) == false);
                    nodesWithLeader.Remove(fastestNode);

                    var choosenNode = new ChoosenNode
                    {
                        Node = fastestNode,
                        SkippedNodes = nodesWithLeader
                    };

                    if (choosenNode.Node != null)
                        return choosenNode;

                    throw new HttpRequestException("Tried all nodes in the cluster but failed getting a response", exception);
                }

                throw new InvalidOperationException($"Invalid ReadBehaviour value: {topology.ReadBehavior}");
            }

            if (topology.WriteBehavior == WriteBehavior.LeaderOnly)
            {
                if (command.IsFailedWithNode(leaderNode) == false)
                    return new ChoosenNode {Node = leaderNode};
                throw new HttpRequestException("Leader not was failed to make this request. The current WriteBehavior is set to Leader to we won't failover to a differnt node.", exception);
            }

            if (topology.WriteBehavior == WriteBehavior.LeaderWithFailover)
            {
                if (leaderNode.IsFailed == false && command.IsFailedWithNode(leaderNode) == false)
                    return new ChoosenNode {Node = leaderNode};

                // TODO: Should we choose nodes here by rate value as for SLA?
                foreach (var node in topology.Nodes)
                {
                    if (node.IsFailed == false && command.IsFailedWithNode(node) == false)
                        return new ChoosenNode { Node = node };
                }

                throw new HttpRequestException("Tried all nodes in the cluster but failed getting a response", exception);
            }

            throw new InvalidOperationException($"Invalid WriteBehavior value: {topology.WriteBehavior}");
        }

        private async Task HandleUnauthorized(string oauthSource, ServerNode node, JsonOperationContext context, bool shouldThrow = true)
        {
            try
            {
                if (string.IsNullOrEmpty(oauthSource))
                    oauthSource = node.Url + "/OAuth/API-Key";

                var currentToken = await _authenticator.AuthenticateAsync(oauthSource, node.ApiKey, context).ConfigureAwait(false);
                node.CurrentToken = currentToken;
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Failed to authorize using api key", e);

                if (shouldThrow)
                    throw;
            }

            if (_updateCurrentTokenTimer == null)
            {
                _updateCurrentTokenTimer = new Timer(UpdateCurrentTokenCallback, null, TimeSpan.FromMinutes(20), TimeSpan.FromMinutes(20));
            }
        }

        private void UpdateCurrentTokenCallback(object _)
        {
            JsonOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var topology = _topology;

                var leaderNode = topology.LeaderNode;
                if (leaderNode != null)
                {
#pragma warning disable 4014
                    HandleUnauthorized(null, leaderNode, context, shouldThrow: false);
#pragma warning restore 4014
                }

                foreach (var node in topology.Nodes)
                {
#pragma warning disable 4014
                    HandleUnauthorized(null, node, context, shouldThrow: false);
#pragma warning restore 4014
                }
            }
        }

        private readonly object _updateFailingNodeStatusLock = new object();

        private void UpdateFailingNodesStatusCallback(object _)
        {
            if (Monitor.TryEnter(_updateFailingNodeStatusLock) == false)
                return;

            try
            {
                var topology = _topology;
                var tasks = new List<Task>();

                var leaderNode = topology.LeaderNode;
                if (leaderNode?.IsFailed ?? false)
                {
                    tasks.Add(TestIfNodeAlive(leaderNode));
                }

                var serverNodes = topology.Nodes;
                if (serverNodes != null)
                {
                    for (var i = 1; i <= serverNodes.Count; i++)
                    {
                        var node = serverNodes[i];
                        if (node?.IsFailed ?? false)
                        {
                            tasks.Add(TestIfNodeAlive(node));
                        }
                    }
                }

                if (tasks.Count == 0)
                    return;

                Task.WaitAll(tasks.ToArray());
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                {
                    Logger.Info("Failed to check if failing server are down", e);
                }
            }
            finally
            {
                Monitor.Exit(_updateFailingNodeStatusLock);
            }
        }

        private async Task TestIfNodeAlive(ServerNode node)
        {
            var command = new GetTopologyCommand();
            string url;
            var request = CreateRequest(node, command, out url);

            var sp = Stopwatch.StartNew();
            try
            {
                var response = await _httpClient.SendAsync(request).ConfigureAwait(false);
                if (response.IsSuccessStatusCode)
                {
                    node.IsFailed = false;
                }
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Tested if node alive but it's down: {url}", e);
            }
            finally
            {
                sp.Stop();
                node.UpdateRequestTime(sp.ElapsedMilliseconds);
            }
        }

        public void Dispose()
        {
            _cache.Dispose();
            _authenticator.Dispose();
            ContextPool.Dispose();
        }
    }
}