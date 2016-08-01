using System;
using System.Globalization;
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

        public ThreadLocal<AggresiveCacheOptions> AggressiveCaching = new ThreadLocal<AggresiveCacheOptions>();

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
                        throw new ErrorResponseException(response, "TODO");
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

        public void Dispose()
        {
            _cache.Dispose();
        }
    }
}