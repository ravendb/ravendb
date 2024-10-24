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

        public static Task<HttpResponseMessage> PostAsync(string requestUri, HttpContent content, CancellationToken token = default)
        {
            return RetryPolicy.ExecuteAsync(t => Instance.PostAsync(requestUri, content, t), token, continueOnCapturedContext: false);
        }

        public static Task<HttpResponseMessage> GetAsync(string requestUri, CancellationToken token = default)
        {
            return RetryPolicy.ExecuteAsync(t => Instance.GetAsync(requestUri, t), token, continueOnCapturedContext: false);
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
