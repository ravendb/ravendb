using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Client.Documents.Commands;
using Sparrow.Json;

namespace Raven.Client.Http
{
    public class RequestExecuter : IDisposable
    {
        public class AggresiveCacheOptions
        {
            public TimeSpan Duration;
        }

        private readonly ApiKeyAuthenticator _authenticator = new ApiKeyAuthenticator();

        public readonly ThreadLocal<AggresiveCacheOptions> AggressiveCaching = new ThreadLocal<AggresiveCacheOptions>();

        public readonly ThreadLocal<string> ApiKey = new ThreadLocal<string>();
        public readonly ThreadLocal<string> ApiKeyToken = new ThreadLocal<string>();

        private readonly HttpCache _cache = new HttpCache();

        private readonly HttpClient _httpClient;

        public RequestExecuter()
        {
            var handler = new HttpClientHandler
            {

            };
            _httpClient = new HttpClient(handler);


        }

        public void Execute(HttpRequestMessage requset)
        {

        }

        public async Task<HttpResponseMessage> ExecuteAsync(HttpRequestMessage requset)
        {
            // _cache.TryGetValue(requset.RequestUri,)

            var response = await _httpClient.SendAsync(requset);
            return response;
        }

        public async Task ExecuteCommandAsync<TResult>(RavenCommand<TResult> command, JsonOperationContext context)
        {
            var request = command.CreateRequest();

            long cachedEtag;
            BlittableJsonReaderObject cachedValue;
            using (var cachedItem = _cache.Get(context, request.RequestUri.ToString(), out cachedEtag, out cachedValue))
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

                using (var response = await ExecuteAsync(request))
                {
                    // read response
                    // error handling
                    if (response.StatusCode == HttpStatusCode.NotModified)
                    {
                        cachedItem.NotModified();
                        command.SetResponse(cachedValue);
                        return;
                    }
                    // TODO: 404
                    // TODO: 401, 403, 412
                    // TODO: 503 <-- & other server down issues
                    // TODO: 500 <-- raise error
                    if (response.IsSuccessStatusCode == false)
                    {
                        switch (response.StatusCode)
                        {
                            case HttpStatusCode.NotFound:
                                // No need to set command result, as it is null
                                return;
                            case HttpStatusCode.Unauthorized:
                            case HttpStatusCode.PreconditionFailed:
                                if (++command.AuthenticationRetries > 1)
                                {
                                    throw new UnauthorizedAccessException("Got unauthorized response exception after trying to authenticate using ApiKey. Please use a valid ApiKey which is enabled on .");
                                }
                                await HandleUnauthorized(response, command.ServerUrl, context).ConfigureAwait(false);
                                await ExecuteCommandAsync(command, context);
                                return;
                            case HttpStatusCode.Forbidden:
                                throw new UnauthorizedAccessException("Forbidan acesses. Make sure you're using the correct ApiKey.");
                            case HttpStatusCode.ServiceUnavailable:
                                // What todo here?
                                break;
                            case HttpStatusCode.InternalServerError:
                                throw new InvalidOperationException($"Got internal server error: {response.ReadErrorResponse()}");
                            default:
                                throw new InvalidOperationException($"Doesn't know how to handle error: {response.StatusCode}, response: {response.ReadErrorResponse()}");
                        }
                    }

                    using (var stream = await response.Content.ReadAsStreamAsync())
                    {
                        using (var blittableJsonReaderObject = await context.ReadForMemoryAsync(stream, "PutResult"))
                        {
                            command.SetResponse(blittableJsonReaderObject);
                            if (response.Headers.ETag != null)
                            {
                                long etag;
                                if (long.TryParse(response.Headers.ETag.Tag, out etag))
                                {
                                    _cache.Set(request.RequestUri.ToString(), etag, blittableJsonReaderObject);
                                }
                            }
                        }
                    }
                }
            }
        }

        private async Task HandleUnauthorized(HttpResponseMessage response, string serverUrl, JsonOperationContext context)
        {
            var apiKey = ApiKey.Value;
            if (string.IsNullOrEmpty(apiKey))
                throw new UnauthorizedAccessException("Got unauthorized response exception. Please specify an API Key.");

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
        }
    }
}