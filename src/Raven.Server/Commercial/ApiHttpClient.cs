using System;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Polly;
using Polly.Retry;
using Raven.Server.Utils;

namespace Raven.Server.Commercial
{
    public static class ApiHttpClient
    {
        public static string ApiRavenDbNet 
        {
            get
            {
                var envValue = Environment.GetEnvironmentVariable("RAVEN_API_ENV");
                
                if (string.IsNullOrWhiteSpace(envValue) == false)
                {
                    return $"https://{envValue}.api.ravendb.net";
                }
                
                return "https://api.ravendb.net";
            }
        }

        private static readonly RavenHttpClient Instance;

        private static readonly AsyncRetryPolicy<HttpResponseMessage> RetryPolicy;

        public static Task<HttpResponseMessage> PostAsync(string relativeUri, HttpContent content, HttpCompletionOption completionOption, bool shouldRetry = true, CancellationToken token = default)
        {
            if (shouldRetry == false)
                return Instance.PostAsync(relativeUri, content, token); 
            
            return RetryPolicy.ExecuteAsync(t =>
            {
                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = content,
                    RequestUri = new Uri(relativeUri, UriKind.Relative)
                };
                return Instance.SendAsync(request, completionOption, t);
            }, token, continueOnCapturedContext: false);
        }

        public static Task<HttpResponseMessage> PostAsync(string relativeUri, HttpContent content, bool shouldRetry = true, CancellationToken token = default) =>
            PostAsync(relativeUri, content, completionOption: HttpCompletionOption.ResponseContentRead, shouldRetry, token);

        public static Task<HttpResponseMessage> GetAsync(string relativeUri, bool shouldRetry = true, CancellationToken token = default)
        {
            if (shouldRetry == false)
                return Instance.GetAsync(relativeUri, token); 
            
            return RetryPolicy.ExecuteAsync(t => Instance.GetAsync(relativeUri, t), token, continueOnCapturedContext: false);
        }

        static ApiHttpClient()
        {
            Instance = new RavenHttpClient
            {
                BaseAddress = new Uri(ApiRavenDbNet)
            };

            RetryPolicy = Policy
                .HandleResult<HttpResponseMessage>(message => message.StatusCode == HttpStatusCode.TooManyRequests && message.Headers.RetryAfter != null)
                .WaitAndRetryAsync(
                    retryCount: 5,
                    sleepDurationProvider: (_, result, _) => result.Result.Headers.RetryAfter.Delta.Value,
                    onRetryAsync: (_, _, _, _) => Task.CompletedTask);
        }
    }
}
