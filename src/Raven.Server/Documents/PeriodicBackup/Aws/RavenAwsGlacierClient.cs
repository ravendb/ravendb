// -----------------------------------------------------------------------
//  <copyright file="RavenAwsGlacierClient.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Server.PeriodicBackup;
using Raven.Client.Util;
using Raven.Server.Exceptions.PeriodicBackup;

namespace Raven.Server.Documents.PeriodicBackup.Aws
{
    public class RavenAwsGlacierClient : RavenAwsClient
    {
        public RavenAwsGlacierClient(string awsAccessKey, string awsSecretKey, string awsRegionName, 
            UploadProgress uploadProgress = null, CancellationToken? cancellationToken = null)
            : base(awsAccessKey, awsSecretKey, awsRegionName, uploadProgress, cancellationToken)
        {
        }

        public async Task<string> UploadArchive(string glacierVaultName, Stream stream, string archiveDescription)
        {
            var url = $"{GetUrl(null)}/-/vaults/{glacierVaultName}/archives";

            var now = SystemTime.UtcNow;

            var payloadHash = RavenAwsHelper.CalculatePayloadHash(stream);
            var payloadTreeHash = RavenAwsHelper.CalculatePayloadTreeHash(stream);

            UploadProgress?.SetTotal(stream.Length);

            // stream is disposed by the HttpClient
            var content = new ProgressableStreamContent(stream, UploadProgress)
            {
                Headers =
                {
                    {"x-amz-glacier-version", "2012-06-01"},
                    {"x-amz-date", RavenAwsHelper.ConvertToString(now)},
                    {"x-amz-content-sha256", payloadHash},
                    {"x-amz-sha256-tree-hash", payloadTreeHash},
                    {"x-amz-archive-description", archiveDescription}
                }
            };

            var headers = ConvertToHeaders(content.Headers, glacierVaultName);

            var client = GetClient(TimeSpan.FromHours(24));
            var authorizationHeaderValue = CalculateAuthorizationHeaderValue(HttpMethods.Post, url, now, headers);
            client.DefaultRequestHeaders.Authorization = authorizationHeaderValue;

            var response = await client.PostAsync(url, content);
            UploadProgress?.ChangeState(UploadState.Done);
            if (response.IsSuccessStatusCode == false)
                throw StorageException.FromResponseMessage(response);

            return ReadArchiveId(response);
        }

        public override string ServiceName => "glacier";

        public override string GetHost(string glacierVaultName)
        {
            return $"glacier.{AwsRegion}.amazonaws.com";
        }

        private static string ReadArchiveId(HttpResponseMessage response)
        {
            return response.Headers
                .GetValues("x-amz-archive-id")
                .First();
        }

        public async Task TestConnection()
        {
            var url = $"{GetUrl(null)}/-/vaults&limit=1";

            var now = SystemTime.UtcNow;

            var requestMessage = new HttpRequestMessage(HttpMethods.Get, url)
            {
                Headers =
                {
                    {"x-amz-glacier-version", "2012-06-01"},
                    {"x-amz-date", RavenAwsHelper.ConvertToString(now)}
                }
            };

            var headers = ConvertToHeaders(requestMessage.Headers);

            var client = GetClient();
            var authorizationHeaderValue = CalculateAuthorizationHeaderValue(HttpMethods.Get, url, now, headers);
            client.DefaultRequestHeaders.Authorization = authorizationHeaderValue;

            var response = await client.SendAsync(requestMessage);
            if (response.IsSuccessStatusCode == false)
                throw StorageException.FromResponseMessage(response);
        }
    }
}