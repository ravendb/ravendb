using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.AI.Settings;
using Sparrow.Json;

namespace SlowTests.Server.Documents.AI
{
    // Captures each outgoing request and answers it with a canned response, so a test never touches the network.
    internal sealed class CapturingChatCompletionClient : ChatCompletionClient
    {
        private readonly Func<string, HttpResponseMessage> _respond;
        private Dictionary<string, string> _lastHeaders = new(StringComparer.OrdinalIgnoreCase);

        public string LastRequestBody;
        public IMemoryContextPool Pool { get; }

        public CapturingChatCompletionClient(IMemoryContextPool contextPool, AbstractChatCompletionProvider provider, Func<string, HttpResponseMessage> respond)
            : base(contextPool, provider, ConventionsToUse)
        {
            _respond = respond;
            Pool = contextPool;
        }

        public string LastHeader(string name) => _lastHeaders.TryGetValue(name, out var value) ? value : null;

        public static HttpResponseMessage Ok(string json) =>
            new(HttpStatusCode.OK) { Content = new StringContent(json, Encoding.UTF8, "application/json") };

        public static HttpResponseMessage Sse(string sse) =>
            new(HttpStatusCode.OK) { Content = new StringContent(sse, Encoding.UTF8, "text/event-stream") };

        protected override Task<HttpResponseMessage> SendRequestAsync(HttpRequestMessage request, CancellationToken token) => Capture(request, token);

        protected override Task<HttpResponseMessage> SendStreamingRequestAsync(HttpRequestMessage request, CancellationToken token) => Capture(request, token);

        private async Task<HttpResponseMessage> Capture(HttpRequestMessage request, CancellationToken token)
        {
            LastRequestBody = request.Content != null ? await request.Content.ReadAsStringAsync(token) : null;

            // the request is disposed once it is sent, so copy the headers out now
            var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var header in request.Headers)
                headers[header.Key] = string.Join(", ", header.Value);
            _lastHeaders = headers;

            return _respond(LastRequestBody);
        }
    }
}
