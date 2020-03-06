// -----------------------------------------------------------------------
//  <copyright file="RavenAwsGlacierClient.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Util;
using Raven.Server.Exceptions.PeriodicBackup;
using Sparrow.Binary;
using Sparrow.Logging;

namespace Raven.Server.Documents.PeriodicBackup.Aws
{
    public class RavenAwsGlacierClient : RavenAwsClient
    {
        private const int MaxUploadArchiveSizeInBytes = 256 * 1024 * 1024; // 256MB
        private const int MinOnePartUploadSizeLimitInBytes = 128 * 1024 * 1024; // 128MB
        private const long MultiPartUploadLimitInBytes = 40L * 1024 * 1024 * 1024 * 1024; // 40TB

        private readonly string _vaultName;
        private readonly Logger _logger;

        public RavenAwsGlacierClient(GlacierSettings glacierSettings, Progress progress = null, Logger logger = null, CancellationToken? cancellationToken = null)
            : base(glacierSettings, progress, cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(glacierSettings.VaultName))
                throw new ArgumentException("AWS vault name can't be null or empty");

            if (string.IsNullOrWhiteSpace(glacierSettings.AwsRegionName))
                throw new ArgumentException("AWS region cannot be null or empty");
            
            AwsRegion = glacierSettings.AwsRegionName.ToLower();
            _vaultName = glacierSettings.VaultName;
            _logger = logger;
        }

        public string UploadArchive(Stream stream, string archiveDescription)
        {
            TestConnection();

            if (stream.Length > MaxUploadArchiveSizeInBytes)
            {
                // for objects over 256MB
                return MultiPartUpload(archiveDescription, stream);
            }

            var url = $"{GetUrl()}/archives";
            var now = SystemTime.UtcNow;

            Progress?.UploadProgress.SetTotal(stream.Length);

            // stream is disposed by the HttpClient
            var content = new ProgressableStreamContent(stream, Progress)
            {
                Headers =
                {
                    {"x-amz-sha256-tree-hash", RavenAwsHelper.CalculatePayloadTreeHash(stream)},
                    {"x-amz-archive-description", archiveDescription},
                    {"Content-Length", stream.Length.ToString(CultureInfo.InvariantCulture)}
                }
            };

            UpdateHeaders(content.Headers, now, stream);

            var headers = ConvertToHeaders(content.Headers);
            var client = GetClient(TimeSpan.FromHours(24));
            var authorizationHeaderValue = CalculateAuthorizationHeaderValue(HttpMethods.Post, url, now, headers);
            client.DefaultRequestHeaders.Authorization = authorizationHeaderValue;

            var response = client.PostAsync(url, content, CancellationToken).Result;
            Progress?.UploadProgress.ChangeState(UploadState.Done);
            if (response.IsSuccessStatusCode == false)
                throw StorageException.FromResponseMessage(response);

            return ReadArchiveId(response);
        }

        private string MultiPartUpload(string archiveDescription, Stream stream)
        {
            var streamLength = stream.Length;
            if (streamLength > MultiPartUploadLimitInBytes)
                throw new InvalidOperationException(@"Can't upload more than 40TB to Amazon Glacier, " +
                                                    $"current upload size: {new Size(streamLength).HumaneSize}");

            Progress?.UploadProgress.SetTotal(streamLength);
            Progress?.UploadProgress.ChangeType(UploadType.Chunked);

            // using a chunked upload we can upload up to 10,000 chunks, 4GB max each
            // we limit every chunk to a minimum of 128MB
            // constraints: the part size must be a megabyte(1024KB) 
            // multiplied by a power of 2-for example, 
            // 1048576(1MB), 2097152(2MB), 4194304(4MB), 8388608(8 MB), and so on.
            // the minimum allowable part size is 1MB and the maximum is 4GB(4096 MB).
            var maxLengthPerPart = Math.Max(MinOnePartUploadSizeLimitInBytes, stream.Length / 10000);
            const long maxPartLength = 4L * 1024 * 1024 * 1024; // 4GB
            var lengthPerPartPowerOf2 = Math.Min(GetNextPowerOf2(maxLengthPerPart), maxPartLength);

            var baseUrl = $"{GetUrl()}/multipart-uploads";
            var uploadId = GetUploadId(baseUrl, archiveDescription, lengthPerPartPowerOf2);
            var client = GetClient(TimeSpan.FromDays(7));

            var uploadUrl = $"{baseUrl}/{uploadId}";
            var fullStreamPayloadTreeHash = RavenAwsHelper.CalculatePayloadTreeHash(stream);

            try
            {
                while (stream.Position < streamLength)
                {
                    var length = Math.Min(lengthPerPartPowerOf2, streamLength - stream.Position);
                    UploadPart(stream, client, uploadUrl, length, retryCount: 0);
                }

                return CompleteMultiUpload(uploadUrl, client, streamLength, fullStreamPayloadTreeHash);
            }
            catch (Exception)
            {
                AbortMultiUpload(uploadUrl, client);
                throw;
            }
            finally
            {
                Progress?.UploadProgress.ChangeState(UploadState.Done);
            }
        }

        private void UploadPart(Stream baseStream, HttpClient client, string url, long length, int retryCount)
        {
            // saving the position if we need to retry
            var position = baseStream.Position;
            using (var subStream = new SubStream(baseStream, offset: 0, length: length))
            {
                var now = SystemTime.UtcNow;

                // stream is disposed by the HttpClient
                var content = new ProgressableStreamContent(subStream, Progress)
                {
                    Headers =
                    {
                        {"x-amz-sha256-tree-hash", RavenAwsHelper.CalculatePayloadTreeHash(subStream)},
                        {"Content-Range", $"bytes {position}-{position+length-1}/*"},
                        {"Content-Length", subStream.Length.ToString(CultureInfo.InvariantCulture)}
                    }
                };

                UpdateHeaders(content.Headers, now, subStream);

                var headers = ConvertToHeaders(content.Headers);
                client.DefaultRequestHeaders.Authorization = CalculateAuthorizationHeaderValue(HttpMethods.Put, url, now, headers);

                try
                {
                    var response = client.PutAsync(url, content, CancellationToken).Result;
                    if (response.IsSuccessStatusCode)
                        return;

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
            CancellationToken.WaitHandle.WaitOne(1000);
            CancellationToken.ThrowIfCancellationRequested();

            retryCount++;

            if (_logger?.IsInfoEnabled == true)
                _logger.Info($"Trying to send the request again. Retries count: '{retryCount}', VaultName: '{_vaultName}'.");

            // restore the stream position before retrying
            baseStream.Position = position;
            UploadPart(baseStream, client, url, length, retryCount);
        }

        private string GetUploadId(string url, string archiveDescription, long lengthPerPartPowerOf2)
        {
            var now = SystemTime.UtcNow;
            var requestMessage = new HttpRequestMessage(HttpMethods.Post, url)
            {
                Headers =
                {
                    {"x-amz-archive-description", archiveDescription},
                    {"x-amz-part-size", lengthPerPartPowerOf2.ToString()}
                }
            };

            UpdateHeaders(requestMessage.Headers, now, null);

            var headers = ConvertToHeaders(requestMessage.Headers);

            var client = GetClient();
            client.DefaultRequestHeaders.Authorization = CalculateAuthorizationHeaderValue(HttpMethods.Post, url, now, headers);

            var response = client.SendAsync(requestMessage, CancellationToken).Result;
            if (response.IsSuccessStatusCode == false)
                throw StorageException.FromResponseMessage(response);

            return response.Headers
                .GetValues("x-amz-multipart-upload-id")
                .First();
        }

        private static long GetNextPowerOf2(long number)
        {
            if (number > 0 && (number & (number - 1)) == 0)
            {
                // already a power of 2
                return number;
            }

            return Bits.PowerOf2(number);
        }

        private string CompleteMultiUpload(string url, HttpClient client, long archiveSize, string payloadTreeHash)
        {
            var now = SystemTime.UtcNow;
            var requestMessage = new HttpRequestMessage(HttpMethods.Post, url)
            {
                Headers =
                {
                    {"x-amz-archive-size", archiveSize.ToString()},
                    {"x-amz-sha256-tree-hash", payloadTreeHash}
                }
            };

            UpdateHeaders(requestMessage.Headers, now, null);

            var headers = ConvertToHeaders(requestMessage.Headers);
            var authorizationHeaderValue = CalculateAuthorizationHeaderValue(HttpMethods.Post, url, now, headers);
            client.DefaultRequestHeaders.Authorization = authorizationHeaderValue;

            var response = client.SendAsync(requestMessage, CancellationToken).Result;
            if (response.IsSuccessStatusCode == false)
                throw StorageException.FromResponseMessage(response);

            return ReadArchiveId(response);
        }

        private void AbortMultiUpload(string url, HttpClient client)
        {
            var now = SystemTime.UtcNow;
            var requestMessage = new HttpRequestMessage(HttpMethods.Delete, url);
            UpdateHeaders(requestMessage.Headers, now, null);

            var headers = ConvertToHeaders(requestMessage.Headers);
            client.DefaultRequestHeaders.Authorization = CalculateAuthorizationHeaderValue(HttpMethods.Delete, url, now, headers);

            var response = client.SendAsync(requestMessage, CancellationToken).Result;
            if (response.IsSuccessStatusCode)
                return;

            // The specified multipart upload does not exist. 
            // The upload ID might be invalid, 
            // or the multipart upload might have been aborted or completed.
            if (response.StatusCode == HttpStatusCode.NotFound)
                return;

            throw StorageException.FromResponseMessage(response);
        }

        public void TestConnection()
        {
            if (VaultExists())
                return;

            throw new VaultNotFoundException($"Vault name '{_vaultName}' doesn't exist in {AwsRegion}!");
        }

        public void PutVault()
        {
            var url = GetUrl();
            var now = SystemTime.UtcNow;
            var content = new HttpRequestMessage(HttpMethods.Put, url);
            UpdateHeaders(content.Headers, now, null);

            var headers = ConvertToHeaders(content.Headers);
            var client = GetClient();
            var authorizationHeaderValue = CalculateAuthorizationHeaderValue(HttpMethods.Put, url, now, headers);
            client.DefaultRequestHeaders.Authorization = authorizationHeaderValue;

            var response = client.SendAsync(content, CancellationToken).Result;
            if (response.IsSuccessStatusCode)
                return;

            throw StorageException.FromResponseMessage(response);
        }

        public void DeleteVault()
        {
            var url = GetUrl();
            var now = SystemTime.UtcNow;
            var content = new HttpRequestMessage(HttpMethods.Delete, url);
            UpdateHeaders(content.Headers, now, null);

            var headers = ConvertToHeaders(content.Headers);

            var client = GetClient();
            var authorizationHeaderValue = CalculateAuthorizationHeaderValue(HttpMethods.Delete, url, now, headers);
            client.DefaultRequestHeaders.Authorization = authorizationHeaderValue;

            var response = client.SendAsync(content, CancellationToken).Result;
            if (response.IsSuccessStatusCode)
                return;

            if (response.StatusCode == HttpStatusCode.NotFound)
                return;

            throw StorageException.FromResponseMessage(response);
        }

        private bool VaultExists()
        {
            var url = GetUrl();
            var now = SystemTime.UtcNow;
            var content = new HttpRequestMessage(HttpMethods.Get, url);
            UpdateHeaders(content.Headers, now, null);

            var headers = ConvertToHeaders(content.Headers);

            var client = GetClient();
            var authorizationHeaderValue = CalculateAuthorizationHeaderValue(HttpMethods.Get, url, now, headers);
            client.DefaultRequestHeaders.Authorization = authorizationHeaderValue;

            var response = client.SendAsync(content, CancellationToken).Result;
            if (response.IsSuccessStatusCode)
                return true;

            if (response.StatusCode == HttpStatusCode.NotFound)
                return false;

            throw StorageException.FromResponseMessage(response);
        }

        public void DeleteArchive(string archiveId)
        {
            var url = $"{GetUrl()}/archives/{archiveId}";
            var now = SystemTime.UtcNow;
            var content = new HttpRequestMessage(HttpMethods.Delete, url);
            UpdateHeaders(content.Headers, now, null);

            var headers = ConvertToHeaders(content.Headers);

            var client = GetClient();
            var authorizationHeaderValue = CalculateAuthorizationHeaderValue(HttpMethods.Delete, url, now, headers);
            client.DefaultRequestHeaders.Authorization = authorizationHeaderValue;

            var response = client.SendAsync(content, CancellationToken).Result;
            if (response.IsSuccessStatusCode)
                return;

            throw StorageException.FromResponseMessage(response);
        }

        private static string ReadArchiveId(HttpResponseMessage response)
        {
            return response.Headers
                .GetValues("x-amz-archive-id")
                .First();
        }

        protected override void UpdateHeaders(HttpHeaders headers, DateTime now, Stream stream, string payloadHash = null)
        {
            base.UpdateHeaders(headers, now, stream, payloadHash);

            headers.Add("x-amz-glacier-version", "2012-06-01");
        }

        public override string ServiceName => "glacier";

        public override string GetUrl()
        {
            var baseUrl = base.GetUrl();
            return $"{baseUrl}/-/vaults/{_vaultName}";
        }

        public override string GetHost()
        {
            return $"glacier.{AwsRegion}.amazonaws.com";
        }
    }
}
