using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Replication;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Documents.Commands;
using Sparrow.Json;
using System.Linq;
using Raven.Abstractions.Exceptions;
using Raven.Client.Exceptions;

namespace Raven.Client.Http
{
    public class RequestExecuter : IDisposable
    {
        private readonly DocumentStore _store;
        private readonly UnmanagedBuffersPool _pool = new UnmanagedBuffersPool("GetTopology");
        private readonly JsonOperationContext _context;

        public class AggresiveCacheOptions
        {
            public TimeSpan Duration;
        }

        private readonly ApiKeyAuthenticator _authenticator = new ApiKeyAuthenticator();

        public readonly ThreadLocal<AggresiveCacheOptions> AggressiveCaching = new ThreadLocal<AggresiveCacheOptions>();

        public readonly ThreadLocal<string> ApiKey = new ThreadLocal<string>();

        private readonly HttpCache _cache = new HttpCache();

        private readonly HttpClient _httpClient;

        private Topology _topology;
        private readonly Timer _updateTopologyTimer;
        private bool _firstTimeTryLoadFromTopologyCache = true;

        public RequestExecuter(DocumentStore store)
        {
            _store = store;
            var handler = new HttpClientHandler
            {
                
            };
            _httpClient = new HttpClient(handler);

            _context = new JsonOperationContext(_pool);

            _updateTopologyTimer = new Timer(UpdateTopologyCallback, null, 0, Timeout.Infinite);
        }

        private void UpdateTopologyCallback(object _)
        {
            // Use server side conventions
            if (_store.Conventions.FailoverBehavior == FailoverBehavior.FailImmediately)
                return;

            var url = _topology?.LeaderNode?.Url ?? _store.Url;
            var database = _topology?.LeaderNode?.Database ?? _store.DefaultDatabase;
            if (url == null || database == null)
                return;

            var serverHash = ServerHash.GetServerHash(url, database);

            if (_firstTimeTryLoadFromTopologyCache)
            {
                _firstTimeTryLoadFromTopologyCache = false;

                // TODO: Avoid string allocation
                _topology = TopologyLocalCache.TryLoadTopologyFromLocalCache(serverHash, _context);
                if (_topology != null)
                {
                    _updateTopologyTimer.Change(TimeSpan.FromSeconds(30), Timeout.InfiniteTimeSpan);
                    return;
                }
            }

            var command = new GetTopologyCommand();
            ExecuteAsync(url, database, _context, command)
                .ContinueWith(task =>
                {
                    _topology = command.Result;
                    TopologyLocalCache.TrySavingTopologyToLocalCache(serverHash, _topology, _context);
                    _updateTopologyTimer.Change(TimeSpan.FromMinutes(5), Timeout.InfiniteTimeSpan);
                });
        }

        public async Task ExecuteAsync<TResult>(string serverUrl, string database, JsonOperationContext context, RavenCommand<TResult> command)
        {
            string url;
            var request = command.CreateRequest(out url);
            url = $"{serverUrl}/databases/{database}/{url}";
            request.RequestUri = new Uri(url);

            long cachedEtag;
            BlittableJsonReaderObject cachedValue;
            using (var cachedItem = _cache.Get(context, url, out cachedEtag, out cachedValue))
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
                    if (_topology == null)
                        throw;

                    if (_topology.LeaderNode.Match(url, database) == false && _topology.LeaderNode.LastFailure.AddMinutes(5) < SystemTime.UtcNow)
                    {
                        await ExecuteAsync(_topology.LeaderNode.Url, _topology.LeaderNode.Database, context, command);
                        return;
                    }

                    foreach (var node in _topology.Nodes)
                    {
                        if (node.Match(url, database) == false && node.LastFailure.AddMinutes(5) < SystemTime.UtcNow)
                        {
                            await ExecuteAsync(_topology.LeaderNode.Url, _topology.LeaderNode.Database, context, command);
                            return;
                        }
                    }

                    throw new HttpRequestException("Tried all nodes in the cluster but failed getting a response", e);
                }

                using (response)
                {
                    // read response
                    // error handling
                    if (response.StatusCode == HttpStatusCode.NotModified)
                    {
                        cachedItem.NotModified();
                        command.SetResponse(cachedValue);
                        return;
                    }
                    if (response.IsSuccessStatusCode == false)
                    {
                        switch (response.StatusCode)
                        {
                            case HttpStatusCode.NotFound:
                                command.SetResponse(null);
                                return;
                            case HttpStatusCode.Unauthorized:
                            case HttpStatusCode.PreconditionFailed:
                                var apiKey = ApiKey.Value;
                                if (string.IsNullOrEmpty(apiKey))
                                    throw new UnauthorizedAccessException($"Got unauthorized response exception for {url}. Please specify an API Key.");
                                if (++command.AuthenticationRetries > 1)
                                    throw new UnauthorizedAccessException($"Got unauthorized response exception for {url} after trying to authenticate using ApiKey.");
                                await HandleUnauthorized(response, serverUrl, apiKey, context).ConfigureAwait(false);
                                await ExecuteAsync(serverUrl, database, context, command).ConfigureAwait(false);
                                return;
                            case HttpStatusCode.Forbidden:
                                throw new UnauthorizedAccessException($"Forbidan access to {url}. Make sure you're using the correct ApiKey.");
                            case HttpStatusCode.BadGateway:
                            case HttpStatusCode.ServiceUnavailable:
                                // TODO: Replication
                                break;
                            case HttpStatusCode.Conflict:
                                // TODO: Conflict resolution
                                break;
                            default:
                                using (var stream = await response.Content.ReadAsStreamAsync())
                                {
                                    using (var blittableJsonReaderObject = await context.ReadForMemoryAsync(stream, "ErrorResponse"))
                                    {
                                        string error;
                                        if (blittableJsonReaderObject.TryGet("Error", out error) == false)
                                            throw new InvalidOperationException($"Doesn't know how to handle error: {response.StatusCode}, response: {response.ReadErrorResponse()}");

                                        if (response.StatusCode == HttpStatusCode.BadRequest)
                                            throw new BadRequestException(error + ". Response: " + blittableJsonReaderObject);

                                        string indexDefinitionProperty;
                                        if (blittableJsonReaderObject.TryGet(nameof(IndexCompilationException.IndexDefinitionProperty), out indexDefinitionProperty))
                                        {
                                            var indexCompilationException = new IndexCompilationException(error);
                                            blittableJsonReaderObject.TryGet(nameof(IndexCompilationException.IndexDefinitionProperty), out indexCompilationException.IndexDefinitionProperty);
                                            blittableJsonReaderObject.TryGet(nameof(IndexCompilationException.ProblematicText), out indexCompilationException.ProblematicText);
                                            throw indexCompilationException;
                                        }

                                        throw new InternalServerErrorException(error + ". Response: " + blittableJsonReaderObject);
                                    }
                                }
                        }
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

        private async Task HandleUnauthorized(HttpResponseMessage response, string serverUrl, string apiKey, JsonOperationContext context)
        {
            var oauthSource = response.Headers.GetFirstValue("OAuth-Source");

#if DEBUG && FIDDLER
                // Make sure to avoid a cross DNS security issue, when running with Fiddler
                if (string.IsNullOrEmpty(oauthSource) == false)
                    oauthSource = oauthSource.Replace("localhost:", "localhost.fiddler:");
#endif

            if (string.IsNullOrEmpty(oauthSource))
                oauthSource = serverUrl + "/OAuth/API-Key";

            var currentToken = await _authenticator.AuthenticateAsync(oauthSource, apiKey, context).ConfigureAwait(false);
            _httpClient.DefaultRequestHeaders.Add("Raven-Authorization", currentToken);
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