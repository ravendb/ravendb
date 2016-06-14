// -----------------------------------------------------------------------
//  <copyright file="RavenAwsGlacierClient.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Linq;
using System.Net.Http;
using Raven.Abstractions;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Util;

namespace Raven.Server.Documents.PeriodicExport.Aws
{
    public class RavenAwsGlacierClient : RavenAwsClient
    {
        public RavenAwsGlacierClient(string awsAccessKey, string awsSecretKey, string awsRegionEndpoint)
            : base(awsAccessKey, awsSecretKey, awsRegionEndpoint)
        {
        }

        public string UploadArchive(string glacierVaultName, Stream stream, string archiveDescription, int timeoutInSeconds)
        {
            var url = string.Format("{0}/-/vaults/{1}/archives", GetUrl(null), glacierVaultName);

            var now = SystemTime.UtcNow;

            var payloadHash = RavenAwsHelper.CalculatePayloadHash(stream);
            var payloadTreeHash = RavenAwsHelper.CalculatePayloadTreeHash(stream);

            var content = new StreamContent(stream)
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

            var headers = ConvertToHeaders(glacierVaultName, content.Headers);

            var client = GetClient(TimeSpan.FromSeconds(timeoutInSeconds));
            var authorizationHeaderValue = CalculateAuthorizationHeaderValue(HttpMethods.Post, url, now, headers);
            client.DefaultRequestHeaders.Authorization = authorizationHeaderValue;

            var response = AsyncHelpers.RunSync(() => client.PostAsync(url, content));
            if (response.IsSuccessStatusCode)
                return ReadArchiveId(response);

            throw ErrorResponseException.FromResponseMessage(response);
        }

        public override string ServiceName => "glacier";

        public override string GetHost(string glacierVaultName)
        {
            return string.Format("glacier.{0}.amazonaws.com", AwsRegion);
        }

        private static string ReadArchiveId(HttpResponseMessage response)
        {
            return response.Headers
                .GetValues("x-amz-archive-id")
                .First();
        }
    }
}