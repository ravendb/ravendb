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
using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Raven.Abstractions;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Util;

namespace Raven.Database.Client.Aws
{
    public class RavenAwsS3Client : RavenAwsClient
    {
        private AmazonS3Client _client;

        private const long OneKb = 1024;
        public const long OneMb = OneKb * 1024;
        private const long OneGb = OneMb * 1024;
        private const long OneTb = OneGb * 1024;
        internal long MaxUploadPutObjectInBytes = 256 * OneMb;
        internal long MinOnePartUploadSizeLimitInBytes = 100 * OneMb;
        private static readonly long TotalBlocksSizeLimitInBytes = 5 * OneTb;

        public RavenAwsS3Client(string awsAccessKey, string awsSecretKey, string awsRegionEndpoint)
            : base(awsAccessKey, awsSecretKey, awsRegionEndpoint)
        {
            if (string.IsNullOrWhiteSpace(awsRegionEndpoint))
                throw new ArgumentException("AWS Region Name cannot be null or empty");

            AmazonS3Config config = new AmazonS3Config
            {
                RegionEndpoint = RegionEndpoint.GetBySystemName(awsRegionEndpoint)
            };

            var credentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
            _client = new AmazonS3Client(credentials, config);
        }

        public void PutObject(string bucketName, string key, Stream stream, Dictionary<string, string> metadata, int timeoutInSeconds)
        {
            AsyncHelpers.RunSync(() => PutObjectAsync(bucketName, key, stream, metadata));
        }

        public async Task PutObjectAsync(string bucketName, string key, Stream stream, Dictionary<string, string> metadata)
        {
            var streamLengthInBytes = stream.Length;
            if (streamLengthInBytes > TotalBlocksSizeLimitInBytes)
                throw new InvalidOperationException("Can't upload more than 5TB to AWS S3, current upload size: " + streamLengthInBytes);

            if (streamLengthInBytes > MaxUploadPutObjectInBytes)
            {
                var multipartRequest = new InitiateMultipartUploadRequest {Key = key, BucketName = bucketName};

                FillMetadata(multipartRequest.Metadata, metadata);

                var initiateResponse = await _client.InitiateMultipartUploadAsync(multipartRequest).ConfigureAwait(false);
                var partNumber = 1;
                var partEtags = new List<PartETag>();

                while (stream.Position < streamLengthInBytes)
                {
                    var leftToUpload = streamLengthInBytes - stream.Position;
                    var toUpload = Math.Min(MinOnePartUploadSizeLimitInBytes, leftToUpload);

                    var uploadResponse = await _client
                        .UploadPartAsync(new UploadPartRequest
                        {
                            Key = key,
                            BucketName = bucketName,
                            InputStream = stream,
                            PartNumber = partNumber++,
                            PartSize = toUpload,
                            UploadId = initiateResponse.UploadId,
                        }).ConfigureAwait(false);

                    partEtags.Add(new PartETag(uploadResponse.PartNumber, uploadResponse.ETag));
                }

                await _client.CompleteMultipartUploadAsync(
                    new CompleteMultipartUploadRequest {UploadId = initiateResponse.UploadId, BucketName = bucketName, Key = key, PartETags = partEtags}).ConfigureAwait(false);

                return;
            }

            var request = new PutObjectRequest
            {
                Key = key,
                BucketName = bucketName,
                InputStream = stream
            };

            FillMetadata(request.Metadata, metadata);

            await _client.PutObjectAsync(request).ConfigureAwait(false);
        }

        private static void FillMetadata(MetadataCollection collection, IDictionary<string, string> metadata)
        {
            if (metadata == null)
                return;

            foreach (var kvp in metadata)
                collection[Uri.EscapeDataString(kvp.Key)] = Uri.EscapeDataString(kvp.Value);
        }

        public Blob GetObject(string bucketName, string key)
        {
            var url = GetUrl(bucketName) + "/" + key;

            var now = SystemTime.UtcNow;

            var payloadHash = RavenAwsHelper.CalculatePayloadHash(null);

            var requestMessage = new HttpRequestMessage(HttpMethods.Get, url)
                                 {
                                     Headers =
                                     {
                                         { "x-amz-date", RavenAwsHelper.ConvertToString(now) },
                                         { "x-amz-content-sha256", payloadHash }
                                     }
                                 };

            var headers = ConvertToHeaders(bucketName, requestMessage.Headers);

            var client = GetClient();
            client.DefaultRequestHeaders.Authorization = CalculateAuthorizationHeaderValue(HttpMethods.Get, url, now, headers);

            var response = AsyncHelpers.RunSync(() => client.SendAsync(requestMessage));
            if (response.StatusCode == HttpStatusCode.NotFound)
                return null;

            if (response.IsSuccessStatusCode == false)
                throw ErrorResponseException.FromResponseMessage(response);

            var data = AsyncHelpers.RunSync(() => response.Content.ReadAsStreamAsync());
            var metadataHeaders = response.Headers.ToDictionary(x => x.Key, x => x.Value.FirstOrDefault());

            return new Blob(data, metadataHeaders);
        }

        public override string ServiceName
        {
            get
            {
                return "s3";
            }
        }

        public override string GetHost(string bucketName)
        {
            if (AwsRegion == "us-east-1")
                return string.Format("{0}.s3.amazonaws.com", bucketName);

            return string.Format("{0}.s3-{1}.amazonaws.com", bucketName, AwsRegion);
        }

        public override void Dispose()
        {
            base.Dispose();

            _client?.Dispose();
            _client = null;
        }
    }
}
