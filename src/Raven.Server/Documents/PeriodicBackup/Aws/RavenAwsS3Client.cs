// -----------------------------------------------------------------------
//  <copyright file="RavenAwsClient.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Util;
using Raven.Server.Exceptions.PeriodicBackup;

namespace Raven.Server.Documents.PeriodicBackup.Aws
{
    public class RavenAwsS3Client : RavenAwsClient
    {
        private const int MaxUploadPutObjectSizeInBytes = 256 * 1024 * 1024; // 256MB
        private const int MinOnePartUploadSizeLimitInBytes = 100 * 1024 * 1024; // 100MB
        private const long MultiPartUploadLimitInBytes = 5L * 1024 * 1024 * 1024 * 1024; // 5TB

        private readonly string _bucketName;

        public RavenAwsS3Client(string awsAccessKey, string awsSecretKey, string awsRegionName,
            string bucketName, Progress progress = null, CancellationToken? cancellationToken = null)
            : base(awsAccessKey, awsSecretKey, awsRegionName, progress, cancellationToken)
        {
            _bucketName = bucketName;
        }

        public async Task PutObject(string key, Stream stream, Dictionary<string, string> metadata)
        {
            await TestConnection();

            if (stream.Length > MaxUploadPutObjectSizeInBytes)
            {
                // for objects over 256MB
                await MultiPartUpload(key, stream, metadata);
                return;
            }

            var url = $"{GetUrl()}/{key}";
            var now = SystemTime.UtcNow;
            var payloadHash = RavenAwsHelper.CalculatePayloadHash(stream);
            Progress?.UploadProgress.SetTotal(stream.Length);

            // stream is disposed by the HttpClient
            var content = new ProgressableStreamContent(stream, Progress)
            {
                Headers =
                {
                    {"x-amz-date", RavenAwsHelper.ConvertToString(now)},
                    {"x-amz-content-sha256", payloadHash}
                }
            };

            foreach (var metadataKey in metadata.Keys)
                content.Headers.Add("x-amz-meta-" + metadataKey.ToLower(), metadata[metadataKey]);

            var headers = ConvertToHeaders(content.Headers);

            var client = GetClient(TimeSpan.FromHours(24));
            var authorizationHeaderValue = CalculateAuthorizationHeaderValue(HttpMethods.Put, url, now, headers);
            client.DefaultRequestHeaders.Authorization = authorizationHeaderValue;

            var response = await client.PutAsync(url, content, CancellationToken);
            Progress?.UploadProgress.ChangeState(UploadState.Done);
            if (response.IsSuccessStatusCode)
                return;

            throw StorageException.FromResponseMessage(response);
        }

        private async Task MultiPartUpload(string key, Stream stream, Dictionary<string, string> metadata)
        {
            var streamLength = stream.Length;
            if (streamLength > MultiPartUploadLimitInBytes)
                throw new InvalidOperationException(@"Can't upload more than 5TB to Amazon S3, " +
                                                    $"current upload size: {new Size(streamLength).HumaneSize}");

            Progress?.UploadProgress.SetTotal(streamLength);
            Progress?.UploadProgress.ChangeType(UploadType.Chunked);

            var baseUrl = $"{GetUrl()}/{key}";
            var uploadId = await GetUploadId(baseUrl, metadata);
            var client = GetClient(TimeSpan.FromDays(7));
            var partNumbersWithEtag = new List<Tuple<int, string>>();
            var partNumber = 0;
            var completeUploadUrl = $"{baseUrl}?uploadId={uploadId}";

            // using a chunked upload we can upload up to 1000 chunks, 5GB max each
            // we limit every chunk to a minimum of 100MB
            var maxLengthPerPart = Math.Max(MinOnePartUploadSizeLimitInBytes, stream.Length / 1000);
            try
            {
                while (stream.Position < streamLength)
                {
                    var length = Math.Min(maxLengthPerPart, streamLength - stream.Position);
                    var url = $"{baseUrl}?partNumber={++partNumber}&uploadId={uploadId}";

                    var etag = await UploadPart(stream, client, url, length, retryCount: 0);
                    partNumbersWithEtag.Add(new Tuple<int, string>(partNumber, etag));
                }

                await CompleteMultiUpload(completeUploadUrl, client, partNumbersWithEtag);
            }
            catch (Exception)
            {
                await AbortMultiUpload(client, completeUploadUrl);
                throw;
            }
            finally
            {
                Progress?.UploadProgress.ChangeState(UploadState.Done);
            }
        }

        private async Task AbortMultiUpload(HttpClient client, string url)
        {
            var now = SystemTime.UtcNow;
            var payloadHash = RavenAwsHelper.CalculatePayloadHash(null);

            var requestMessage = new HttpRequestMessage(HttpMethods.Delete, url)
            {
                Headers =
                {
                    {"x-amz-date", RavenAwsHelper.ConvertToString(now)},
                    {"x-amz-content-sha256", payloadHash}
                }
            };

            var headers = ConvertToHeaders(requestMessage.Headers);

            client.DefaultRequestHeaders.Authorization = CalculateAuthorizationHeaderValue(HttpMethods.Delete, url, now, headers);

            var response = await client.SendAsync(requestMessage, CancellationToken);
            if (response.IsSuccessStatusCode)
                return;

            // The specified multipart upload does not exist. 
            // The upload ID might be invalid, 
            // or the multipart upload might have been aborted or completed.
            if (response.StatusCode == HttpStatusCode.NotFound)
                return;

            throw StorageException.FromResponseMessage(response);
        }

        private async Task CompleteMultiUpload(string url, HttpClient client,
            List<Tuple<int, string>> partNumbersWithEtag)
        {
            var now = SystemTime.UtcNow;
            var doc = CreateCompleteMultiUploadDocument(partNumbersWithEtag);
            var xmlString = doc.OuterXml;
            var payloadHash = RavenAwsHelper.CalculatePayloadHashFromString(xmlString);

            var requestMessage = new HttpRequestMessage(HttpMethods.Post, url)
            {
                Headers =
                {
                    {"x-amz-date", RavenAwsHelper.ConvertToString(now)},
                    {"x-amz-content-sha256", payloadHash}
                },
                Content = new StringContent(xmlString, Encoding.UTF8, "text/plain")
            };

            var headers = ConvertToHeaders(requestMessage.Headers);
            var authorizationHeaderValue = CalculateAuthorizationHeaderValue(HttpMethods.Post, url, now, headers);
            client.DefaultRequestHeaders.Authorization = authorizationHeaderValue;

            var response = await client.SendAsync(requestMessage, CancellationToken);
            if (response.IsSuccessStatusCode)
                return;

            throw StorageException.FromResponseMessage(response);
        }

        private static XmlDocument CreateCompleteMultiUploadDocument(List<Tuple<int, string>> partNumbersWithEtag)
        {
            var doc = new XmlDocument();
            var xmlDeclaration = doc.CreateXmlDeclaration("1.0", "UTF-8", null);
            var root = doc.DocumentElement;
            doc.InsertBefore(xmlDeclaration, root);

            var completeMultipartUpload = doc.CreateElement("CompleteMultipartUpload");
            doc.AppendChild(completeMultipartUpload);

            foreach (var partNumberWithEtag in partNumbersWithEtag)
            {
                var part = doc.CreateElement("Part");

                var partNumber = doc.CreateElement("PartNumber");
                var partNumberTextNode = doc.CreateTextNode(partNumberWithEtag.Item1.ToString());
                partNumber.AppendChild(partNumberTextNode);

                var etag = doc.CreateElement("ETag");
                var etagTextNodex = doc.CreateTextNode(partNumberWithEtag.Item2);
                etag.AppendChild(etagTextNodex);

                part.AppendChild(partNumber);
                part.AppendChild(etag);

                completeMultipartUpload.AppendChild(part);
            }

            return doc;
        }

        private async Task<string> UploadPart(Stream baseStream, HttpClient client, string url, long length, int retryCount)
        {
            // saving the position if we need to retry
            var position = baseStream.Position;
            using (var subStream = new SubStream(baseStream, offset: 0, length: length))
            {
                var now = SystemTime.UtcNow;
                var payloadHash = RavenAwsHelper.CalculatePayloadHash(subStream);

                // stream is disposed by the HttpClient
                var content = new ProgressableStreamContent(subStream, Progress)
                {
                    Headers =
                    {
                        {"x-amz-date", RavenAwsHelper.ConvertToString(now)},
                        {"x-amz-content-sha256", payloadHash},
                        {"Content-Length", subStream.Length.ToString(CultureInfo.InvariantCulture)}
                    }
                };

                var headers = ConvertToHeaders(content.Headers);
                client.DefaultRequestHeaders.Authorization = CalculateAuthorizationHeaderValue(HttpMethods.Put, url, now, headers);

                try
                {
                    var response = await client.PutAsync(url, content, CancellationToken);
                    if (response.IsSuccessStatusCode)
                    {
                        var etagHeader = response.Headers.GetValues("ETag");
                        return etagHeader.First();
                    }

                    if (retryCount == MaxRetriesForMultiPartUpload)
                        throw StorageException.FromResponseMessage(response);
                }
                catch (Exception)
                {
                    if (retryCount == MaxRetriesForMultiPartUpload)
                        throw;
                }

                // revert the uploaded count before retry
                Progress?.UploadProgress.UpdateUploaded(-content.Uploaded);
            }

            // wait for one second before trying again to send the request
            // maybe there was a network issue?
            await Task.Delay(1000);

            CancellationToken.ThrowIfCancellationRequested();

            // restore the stream position before retrying
            baseStream.Position = position;
            return await UploadPart(baseStream, client, url, length, ++retryCount);
        }

        private async Task<string> GetUploadId(string baseUrl, Dictionary<string, string> metadata)
        {
            var url = $"{baseUrl}?uploads";
            var now = SystemTime.UtcNow;
            var payloadHash = RavenAwsHelper.CalculatePayloadHash(null);

            var requestMessage = new HttpRequestMessage(HttpMethods.Post, url)
            {
                Headers =
                {
                    {"x-amz-date", RavenAwsHelper.ConvertToString(now)},
                    {"x-amz-content-sha256", payloadHash}
                }
            };

            foreach (var metadataKey in metadata.Keys)
                requestMessage.Headers.Add("x-amz-meta-" + metadataKey.ToLower(), metadata[metadataKey]);

            var headers = ConvertToHeaders(requestMessage.Headers);

            var client = GetClient();
            client.DefaultRequestHeaders.Authorization = CalculateAuthorizationHeaderValue(HttpMethods.Post, url, now, headers);

            var response = await client.SendAsync(requestMessage, CancellationToken);
            if (response.IsSuccessStatusCode == false)
                throw StorageException.FromResponseMessage(response);

            using (var stream = await response.Content.ReadAsStreamAsync())
            using (var reader = new StreamReader(stream))
            {
                var xDocument = XDocument.Load(reader);
                var permissionElement = xDocument
                    .Descendants()
                    .First(x => x.Name.LocalName == "UploadId");
                return permissionElement.Value;
            }
        }

        public async Task TestConnection()
        {
            var bucketLocation = await GetBucketLocation();
            if (bucketLocation.Equals(AwsRegion, StringComparison.OrdinalIgnoreCase) == false)
            {
                throw new InvalidOperationException(
                    $"AWS location is set to {AwsRegion}, " +
                    $"but the bucket named: '{_bucketName}' " +
                    $"is located in: {bucketLocation}");
            }

            var bucketPermission = await GetBucketPermission();
            if (bucketPermission != "FULL_CONTROL" && bucketPermission != "WRITE")
            {
                throw new InvalidOperationException(
                    $"Can't create an objcet in bucket '{_bucketName}', " +
                    $"when permission is set to '{bucketPermission}'");
            }
        }

        private async Task<string> GetBucketLocation(bool returnWhenNotFound = false)
        {
            using (UseRegionInvariantRequest())
            {
                var url = $"{GetUrl()}?location";
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

                var response = await client.SendAsync(requestMessage, CancellationToken);
                if (response.StatusCode == HttpStatusCode.NotFound)
                {
                    if (returnWhenNotFound)
                        return null;

                    throw new BucketNotFoundException($"Bucket name '{_bucketName}' doesn't exist!");
                }

                if (response.IsSuccessStatusCode == false)
                    throw StorageException.FromResponseMessage(response);

                using (var stream = await response.Content.ReadAsStreamAsync())
                using (var reader = new StreamReader(stream))
                {
                    var xElement = XElement.Load(reader);
                    var value = xElement.Value;

                    if (value.Equals(string.Empty))
                    {
                        // when the bucket's region is US East (N. Virginia - us-east-1), 
                        // Amazon S3 returns an empty string for the bucket's region
                        value = DefaultRegion;
                    }
                    else if (value.Equals("EU", StringComparison.OrdinalIgnoreCase))
                    {
                        // EU (Ireland) => EU or eu-west-1
                        value = "eu-west-1";
                    }

                    return value == string.Empty ? DefaultRegion : value;
                }
            }
        }

        private async Task<string> GetBucketPermission()
        {
            var url = $"{GetUrl()}?acl";
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

            var response = await client.SendAsync(requestMessage, CancellationToken);
            if (response.StatusCode == HttpStatusCode.NotFound)
                throw new BucketNotFoundException($"Bucket name '{_bucketName}' doesn't exist!");

            if (response.IsSuccessStatusCode == false)
                throw StorageException.FromResponseMessage(response);

            using (var stream = await response.Content.ReadAsStreamAsync())
            using (var reader = new StreamReader(stream))
            {
                var xDocument = XDocument.Load(reader);
                var permissionElement = xDocument
                    .Descendants()
                    .First(x => x.Name.LocalName == "Permission");
                return permissionElement.Value;
            }
        }

        public async Task PutBucket(string awsRegion = null)
        {
            // we set the bucket region in the request message
            using (UseRegionInvariantRequest())
            {
                var doc = CreatePutBucketXmlDocument(awsRegion ?? AwsRegion);
                var xmlString = doc.OuterXml;
                var url = GetUrl();
                var now = SystemTime.UtcNow;

                var hasLocationConstraint = AwsRegion != DefaultRegion;
                var payloadHash = hasLocationConstraint ?
                    RavenAwsHelper.CalculatePayloadHashFromString(xmlString) :
                    RavenAwsHelper.CalculatePayloadHash(null);

                var requestMessage = new HttpRequestMessage(HttpMethods.Put, url)
                {
                    Headers =
                    {
                        {"x-amz-date", RavenAwsHelper.ConvertToString(now)},
                        {"x-amz-content-sha256", payloadHash}
                    },
                    Content = hasLocationConstraint == false ?
                        null : new StringContent(xmlString, Encoding.UTF8, "text/plain")
                };

                var headers = ConvertToHeaders(requestMessage.Headers);

                var client = GetClient(TimeSpan.FromHours(1));
                var authorizationHeaderValue = CalculateAuthorizationHeaderValue(HttpMethods.Put, url, now, headers);
                client.DefaultRequestHeaders.Authorization = authorizationHeaderValue;

                var response = await client.SendAsync(requestMessage, CancellationToken);
                if (response.IsSuccessStatusCode)
                    return;

                if (response.StatusCode == HttpStatusCode.NotFound)
                    return;

                throw StorageException.FromResponseMessage(response);
            }
        }

        private static XmlDocument CreatePutBucketXmlDocument(string region)
        {
            var doc = new XmlDocument();
            var xmlDeclaration = doc.CreateXmlDeclaration("1.0", "UTF-8", null);
            var root = doc.DocumentElement;
            doc.InsertBefore(xmlDeclaration, root);

            var createBucketConfiguration = doc.CreateElement("CreateBucketConfiguration");
            doc.AppendChild(createBucketConfiguration);
            createBucketConfiguration.SetAttribute("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/");

            var locationConstraint = doc.CreateElement("LocationConstraint");
            var text = doc.CreateTextNode(region);
            locationConstraint.AppendChild(text);
            createBucketConfiguration.AppendChild(locationConstraint);

            return doc;
        }

        public async Task DeleteBucket()
        {
            var region = await GetBucketLocation(returnWhenNotFound: true);
            if (region == null)
                return;

            using (UseSpecificRegion(region))
            {
                var url = GetUrl();
                var now = SystemTime.UtcNow;
                var payloadHash = RavenAwsHelper.CalculatePayloadHash(null);

                var requestMessage = new HttpRequestMessage(HttpMethods.Delete, url)
                {
                    Headers =
                    {
                        {"x-amz-date", RavenAwsHelper.ConvertToString(now)},
                        {"x-amz-content-sha256", payloadHash}
                    }
                };

                var headers = ConvertToHeaders(requestMessage.Headers);

                var client = GetClient();
                client.DefaultRequestHeaders.Authorization = CalculateAuthorizationHeaderValue(HttpMethods.Delete, url, now, headers);

                var response = await client.SendAsync(requestMessage, CancellationToken);
                if (response.IsSuccessStatusCode)
                    return;

                if (response.StatusCode == HttpStatusCode.NotFound)
                    return;

                throw StorageException.FromResponseMessage(response);
            }
        }

        public async Task<Blob> GetObject(string key)
        {
            var url = $"{GetUrl()}/{key}";
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

            var response = await client.SendAsync(requestMessage, CancellationToken);
            if (response.StatusCode == HttpStatusCode.NotFound)
                return null;

            if (response.IsSuccessStatusCode == false)
                throw StorageException.FromResponseMessage(response);

            var data = await response.Content.ReadAsStreamAsync();
            var metadataHeaders = response.Headers.ToDictionary(x => x.Key, x => x.Value.FirstOrDefault());

            return new Blob(data, metadataHeaders);
        }

        public async Task DeleteObject(string key)
        {
            var url = $"{GetUrl()}/{key}";
            var now = SystemTime.UtcNow;
            var payloadHash = RavenAwsHelper.CalculatePayloadHash(null);

            var requestMessage = new HttpRequestMessage(HttpMethods.Delete, url)
            {
                Headers =
                {
                    {"x-amz-date", RavenAwsHelper.ConvertToString(now)},
                    {"x-amz-content-sha256", payloadHash}
                }
            };

            var headers = ConvertToHeaders(requestMessage.Headers);

            var client = GetClient(TimeSpan.FromHours(1));
            var authorizationHeaderValue = CalculateAuthorizationHeaderValue(HttpMethods.Delete, url, now, headers);
            client.DefaultRequestHeaders.Authorization = authorizationHeaderValue;

            var response = await client.SendAsync(requestMessage, CancellationToken);
            if (response.IsSuccessStatusCode)
                return;

            if (response.StatusCode == HttpStatusCode.NotFound)
                return;

            throw StorageException.FromResponseMessage(response);
        }

        public override string ServiceName { get; } = "s3";

        public override string GetUrl()
        {
            var baseUrl = base.GetUrl();
            return $"{baseUrl}/{_bucketName}";
        }

        public override string GetHost()
        {
            if (AwsRegion == DefaultRegion || IsRegionInvariantRequest)
                return "s3.amazonaws.com";

            return $"s3-{AwsRegion}.amazonaws.com";
        }

        public IDisposable UseRegionInvariantRequest()
        {
            IsRegionInvariantRequest = true;
            return new DisposableAction(() => IsRegionInvariantRequest = false);
        }

        public IDisposable UseSpecificRegion(string regionToUse)
        {
            var oldRegion = AwsRegion;
            AwsRegion = regionToUse;
            return new DisposableAction(() => AwsRegion = oldRegion);
        }
    }
}
