// -----------------------------------------------------------------------
//  <copyright file="RavenStorageClient.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Server.Documents.PeriodicBackup
{
    public abstract class RavenStorageClient
    {
        protected static readonly HttpClient HttpClient;

        protected readonly CancellationToken CancellationToken;
        protected readonly Progress Progress;
        protected const int MaxRetriesForMultiPartUpload = 5;

        static RavenStorageClient()
        {
            var handler = new HttpClientHandler
            {
                AutomaticDecompression = System.Net.DecompressionMethods.None
            };

            var client = new HttpClient(handler)
            {
                Timeout = TimeSpan.FromHours(24)
            };

            HttpClient = client;
        }

        protected RavenStorageClient(Progress progress, CancellationToken? cancellationToken)
        {
            Debug.Assert(progress == null || (progress.UploadProgress != null && progress.OnUploadProgress != null));

            Progress = progress;
            CancellationToken = cancellationToken ?? CancellationToken.None;
        }

        protected class SendParameters
        {
            public HttpMethod HttpMethod { get; set; }

            public string Url { get; set; }

            public Dictionary<string, string> RequestHeaders { get; set; }

            public HttpContent HttpContent { get; set; }

            public string PayloadHash { get; set; }

            public HttpCompletionOption HttpCompletionOption { get; set; } = HttpCompletionOption.ResponseContentRead;
        }

        protected async Task<HttpResponseMessage> SendAsync(SendParameters sendParameters)
        {
            var httpRequestMessage = new HttpRequestMessage
            {
                RequestUri = new Uri(sendParameters.Url),
                Method = sendParameters.HttpMethod,
                Content = sendParameters.HttpContent
            };

            if (sendParameters.RequestHeaders != null)
            {
                foreach (var headerKey in sendParameters.RequestHeaders.Keys)
                    httpRequestMessage.Headers.Add(headerKey.ToLower(), sendParameters.RequestHeaders[headerKey]);
            }

            HttpHeaders headersForAuthenticationHeaderValue = httpRequestMessage.Headers;
            var baseHeaders = GetBaseHeaders(sendParameters.PayloadHash, out var now);
            if (sendParameters.HttpContent != null)
            {
                httpRequestMessage.Content = sendParameters.HttpContent;
                headersForAuthenticationHeaderValue = httpRequestMessage.Content.Headers;

                foreach (var header in baseHeaders)
                    httpRequestMessage.Content.Headers.Add(header.Key, header.Value);
            }
            else
            {
                foreach (var header in baseHeaders)
                    httpRequestMessage.Headers.Add(header.Key, header.Value);
            }

            var authenticationHeaderValue = GetAuthenticationHeaderValue(sendParameters.HttpMethod, sendParameters.Url, headersForAuthenticationHeaderValue, now);
            if (authenticationHeaderValue != null)
                httpRequestMessage.Headers.Authorization = authenticationHeaderValue;

            return await HttpClient.SendAsync(httpRequestMessage, sendParameters.HttpCompletionOption, CancellationToken);
        }

        protected abstract Dictionary<string, string> GetBaseHeaders(string payloadHash, out DateTime now);

        public abstract AuthenticationHeaderValue GetAuthenticationHeaderValue(HttpMethod httpMethod, string url, HttpHeaders httpHeaders, DateTime now, HttpContentHeaders httpContentHeaders = null);

        public class Blob
        {
            public Blob(Stream data, Dictionary<string, string> metadata)
            {
                Data = data;
                Metadata = metadata;
            }

            public Stream Data { get; }

            public Dictionary<string, string> Metadata { get; }
        }

        public class ListBlobResult
        {
            public IEnumerable<BlobProperties> List { get; set; }

            public string NextMarker { get; set; }
        }

        public class BlobProperties
        {
            public string Name { get; set; }
        }
    }
}
