using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Replication;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Documents.Commands;
using Sparrow.Json;
using Raven.Abstractions.Exceptions;
using Raven.Client.Exceptions;
using Sparrow.Logging;

namespace Raven.Client.Http
{
    public class RequestExecuter : IDisposable
    {
        private static readonly Logger Logger = LoggerSetup.Instance.GetLogger<RequestExecuter>("Client");

        private readonly DocumentStore _store;
        private readonly UnmanagedBuffersPool _pool = new UnmanagedBuffersPool("client/RequestExecuter");
        private readonly JsonOperationContext _context;

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

        public RequestExecuter(DocumentStore store)
        {
            _store = store;
            _topology = new Topology
            {
                LeaderNode = new ServerNode
                {
                    Database = _store.DefaultDatabase,
                    ApiKey = _store.ApiKey,
                    Url = _store.Url,
                },
                Etag = int.MinValue
            };

            var handler = new HttpClientHandler();
            _httpClient = new HttpClient(handler);

            _context = new JsonOperationContext(_pool);

            _updateTopologyTimer = new Timer(UpdateTopologyCallback, null, 0, Timeout.Infinite);
            _updateFailingNodesStatus = new Timer(UpdateFailingNodesStatusCallback, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
        }

        private void UpdateTopologyCallback(object _)
        {
            // Use server side conventions
            if (_store.Conventions.FailoverBehavior == FailoverBehavior.FailImmediately)
                return;

            var node = _topology.LeaderNode;

            var serverHash = ServerHash.GetServerHash(node.Url, node.Database);

            if (_firstTimeTryLoadFromTopologyCache)
            {
                _firstTimeTryLoadFromTopologyCache = false;

                var cachedTopology = TopologyLocalCache.TryLoadTopologyFromLocalCache(serverHash, _context);
                if (cachedTopology != null)
                {
                    _topology = cachedTopology;
                    // we have cached topology, but we need to verify it is up to date, we'll check in 
                    // 1 second, and let the rest of the system start
                    _updateTopologyTimer.Change(TimeSpan.FromSeconds(1), Timeout.InfiniteTimeSpan);
                    return;
                }
            }

            var command = new GetTopologyCommand();
            ExecuteAsync(node, _context, command)
                .ContinueWith(task =>
                {
                    if (task.IsFaulted == false)
                    {
                        if (_topology.Etag != command.Result.Etag)
                        {
                            _topology = command.Result;
                            TopologyLocalCache.TrySavingTopologyToLocalCache(serverHash, _topology, _context);
                        }
                    }

                    _updateTopologyTimer.Change(TimeSpan.FromMinutes(5), Timeout.InfiniteTimeSpan);
                });
        }

        public async Task ExecuteAsync<TResult>(ServerNode node, JsonOperationContext context, RavenCommand<TResult> command)
        {
            string url;
            var request = CreateRequest(node, command, out url);

            long cachedEtag;
            BlittableJsonReaderObject cachedValue;
            HttpCache.ReleaseCacheItem cachedItem;
            using (cachedItem = _cache.Get(context, url, out cachedEtag, out cachedValue))
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

                HttpResponseMessage response;
                try
                {
                    response = await _httpClient.SendAsync(request).ConfigureAwait(false);
                }
                catch (HttpRequestException e) // server down, network down
                {
                    await HandleServerDown(node, context, command, e);
                    return;
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
                        if (await HandleUnsuccessfulResponse(node, context, command, response, url))
                            return;
                    }

                    using (var stream = await response.Content.ReadAsStreamAsync())
                    {
                        using (var blittableJsonReaderObject = await context.ReadForMemoryAsync(stream, "PutResult"))
                        {
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
        }

        private static HttpRequestMessage CreateRequest<TResult>(ServerNode node, RavenCommand<TResult> command, out string url)
        {
            var request = command.CreateRequest(out url);
            url = $"{node.Url}/databases/{node.Database}/{url}";
            request.RequestUri = new Uri(url);

            if (node.CurrentToken != null)
                request.Headers.Add("Raven-Authorization", node.CurrentToken);

            return request;
        }

        private async Task<bool> HandleUnsuccessfulResponse<TResult>(ServerNode node, JsonOperationContext context, RavenCommand<TResult> command,
            HttpResponseMessage response, string url)
        {
            switch (response.StatusCode)
            {
                case HttpStatusCode.NotFound:
                    command.SetResponse(null);
                    return true;
                case HttpStatusCode.Unauthorized:
                case HttpStatusCode.PreconditionFailed:
                    if (string.IsNullOrEmpty(node.ApiKey))
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

                    await HandleUnauthorized(oauthSource, node, context).ConfigureAwait(false);
                    await ExecuteAsync(node, context, command).ConfigureAwait(false);
                    return true;
                case HttpStatusCode.Forbidden:
                    throw new UnauthorizedAccessException(
                        $"Forbidan access to {url}. Make sure you're using the correct ApiKey.");
                case HttpStatusCode.BadGateway:
                case HttpStatusCode.ServiceUnavailable:
                    await HandleServerDown(node, context, command, null);
                    break;
                case HttpStatusCode.Conflict:
                    // TODO: Conflict resolution
                    break;
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
                catch (InvalidDataException e)
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

        private async Task HandleServerDown<TResult>(ServerNode node, JsonOperationContext context, RavenCommand<TResult> command,
            HttpRequestException e)
        {

            if (command.FailedNodes == null)
                command.FailedNodes = new HashSet<ServerNode>();

            node.IsFailed = true;
            command.FailedNodes.Add(node);

            var failoverNode = GetFailoverNode(command);

            if (failoverNode != null)
            {
                await ExecuteAsync(failoverNode, context, command);
                return;
            }

            throw new HttpRequestException("Tried all nodes in the cluster but failed getting a response", e);
        }

        private ServerNode GetFailoverNode<T>(RavenCommand<T> command)
        {
            //TODO: implement failover policies
            var topology = _topology;

            if (command.FailedNodes == null)
                command.FailedNodes = new HashSet<ServerNode>();

            var leaderNode = topology.LeaderNode;
            if (leaderNode.IsFailed == false && command.FailedNodes.Contains(leaderNode) == false)
                return leaderNode;

            foreach (var node in topology.Nodes)
            {
                if (node.IsFailed == false && command.FailedNodes.Contains(node) == false)
                    return node;
            }
            return null;
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
            var topology = _topology;

            var leaderNode = topology.LeaderNode;
            if (leaderNode != null)
            {
#pragma warning disable 4014
                HandleUnauthorized(null, leaderNode, _context, shouldThrow: false);
#pragma warning restore 4014
            }

            foreach (var node in topology.Nodes)
            {
#pragma warning disable 4014
                HandleUnauthorized(null, node, _context, shouldThrow: false);
#pragma warning restore 4014
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

                for (var i = 1; i <= topology.Nodes.Count; i++)
                {
                    var node = topology.Nodes[i];
                    if (node?.IsFailed ?? false)
                    {
                        tasks.Add(TestIfNodeAlive(node));
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
        }

        public void Dispose()
        {
            _cache.Dispose();
            _authenticator.Dispose();
            _context.Dispose();
            _pool.Dispose();
        }
    }
}