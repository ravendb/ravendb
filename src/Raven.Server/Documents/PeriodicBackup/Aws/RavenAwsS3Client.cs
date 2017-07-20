// -----------------------------------------------------------------------
//  <copyright file="RavenAwsClient.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Util;
using Raven.Server.Exceptions.PeriodicBackup;

namespace Raven.Server.Documents.PeriodicBackup.Aws
{
    public class RavenAwsS3Client : RavenAwsClient
    {
        public RavenAwsS3Client(string awsAccessKey, string awsSecretKey, string awsRegionName)
            : base(awsAccessKey, awsSecretKey, awsRegionName)
        {
        }

        public async Task PutObject(string bucketName, string key, Stream stream, Dictionary<string, string> metadata, int timeoutInSeconds)
        {
            var url = GetUrl(bucketName) + "/" + key;

            var now = SystemTime.UtcNow;

            var payloadHash = RavenAwsHelper.CalculatePayloadHash(stream);

            var content = new StreamContent(stream)
            {
                Headers =
                {
                    {"x-amz-date", RavenAwsHelper.ConvertToString(now)},
                    {"x-amz-content-sha256", payloadHash}
                }
            };

            foreach (var metadataKey in metadata.Keys)
                content.Headers.Add("x-amz-meta-" + metadataKey.ToLower(), metadata[metadataKey]);

            var headers = ConvertToHeaders(content.Headers, bucketName);

            var client = GetClient(TimeSpan.FromSeconds(timeoutInSeconds));
            var authorizationHeaderValue = CalculateAuthorizationHeaderValue(HttpMethods.Put, url, now, headers);
            client.DefaultRequestHeaders.Authorization = authorizationHeaderValue;

            var response = await client.PutAsync(url, content);
            if (response.IsSuccessStatusCode)
                return;

            throw StorageException.FromResponseMessage(response);
        }

        public async Task<Blob> GetObject(string bucketName, string key)
        {
            var url = GetUrl(bucketName) + "/" + key;

            var now = SystemTime.UtcNow;

            var payloadHash = RavenAwsHelper.CalculatePayloadHash(null);

            var requestMessage = new HttpRequestMessage(HttpMethods.Get, url)
            {
                Headers =
                {
                    {"x-amz-date", RavenAwsHelper.ConvertToString(now)},
                    {"x-amz-content-sha256", payloadHash}
                }
            };

            var headers = ConvertToHeaders(requestMessage.Headers, bucketName);

            var client = GetClient();
            client.DefaultRequestHeaders.Authorization = CalculateAuthorizationHeaderValue(HttpMethods.Get, url, now, headers);

            var response = await client.SendAsync(requestMessage);
            if (response.StatusCode == HttpStatusCode.NotFound)
                return null;

            if (response.IsSuccessStatusCode == false)
                throw StorageException.FromResponseMessage(response);

            var data = await response.Content.ReadAsStreamAsync();
            var metadataHeaders = response.Headers.ToDictionary(x => x.Key, x => x.Value.FirstOrDefault());

            return new Blob(data, metadataHeaders);
        }

        public override string ServiceName { get; } = "s3";

        public override string GetHost(string bucketName)
        {
            if (bucketName == null)
                return "s3.amazonaws.com";

            if (AwsRegion == DefaultRegion)
                return $"{bucketName}.s3.amazonaws.com";

            return $"{bucketName}.s3-{AwsRegion}.amazonaws.com";
        }

        public async Task TestConnection()
        {
            var url = GetUrl(null);
            var now = SystemTime.UtcNow;

            var payloadHash = RavenAwsHelper.CalculatePayloadHash(null);

            var requestMessage = new HttpRequestMessage(HttpMethods.Get, url)
            {
                Headers =
                {
                    {"x-amz-date", RavenAwsHelper.ConvertToString(now)},
                    {"x-amz-content-sha256", payloadHash}
                }
            };

            var headers = ConvertToHeaders(requestMessage.Headers);

            var client = GetClient();
            client.DefaultRequestHeaders.Authorization = CalculateAuthorizationHeaderValue(HttpMethods.Get, url, now, headers);

            var response = await client.SendAsync(requestMessage);
            if (response.IsSuccessStatusCode == false)
                throw StorageException.FromResponseMessage(response);
        }
    }
}