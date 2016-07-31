using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class RequestExecuter
    {
        // cache 
        // By url, has etag
        // least recently used
        // track size of objects
        // cleanup cache on size 
        // note that if you use cache, you must have a strong ref to the cache object
        // ref counting for the objects so they would be disposed
        // note that we use unmanaged memory here

            // void SetInCache(string url, long etag, Headers, BlittableJsonObjectReader obj);
            // copy the obj to its own memory 


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
            using (var response = await ExecuteAsync(request))
            {
                // read response
                // error handling
                if (response.IsSuccessStatusCode == false)
                {
                    throw new ErrorResponseException(response, "TODO");
                }

                using (var stream = await response.Content.ReadAsStreamAsync())
                {
                    using (var blittableJsonReaderObject = await context.ReadForMemoryAsync(stream, "PutResult"))
                    {
                        command.SetResponse(blittableJsonReaderObject);
                    }
                }
            }
        }
    }
}