﻿// -----------------------------------------------------------------------
//  <copyright file="RavenAzureClient.cs" company="Hibernating Rhinos LTD">
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
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;
using Microsoft.AspNetCore.WebUtilities;
using Raven.Client;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Util;
using Raven.Server.Documents.PeriodicBackup.DirectUpload;
using Raven.Server.Documents.PeriodicBackup.Restore;
using Raven.Server.Exceptions.PeriodicBackup;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Logging;
using Size = Raven.Client.Util.Size;

namespace Raven.Server.Documents.PeriodicBackup.Azure
{
    public sealed class LegacyRavenAzureClient : RavenStorageClient, IRavenAzureClient
    {
        private readonly bool _hasSasToken;
        private readonly string _accountName;
        private readonly byte[] _accountKey;
        private readonly string _sasToken;
        private readonly string _containerName;
        private readonly string _serverUrlForContainer;
        private readonly string _serverUrlForAccountName;
        private const string AzureStorageVersion = "2019-02-02";
        private const long TotalBlocksSizeLimitInBytes = 475L * 1024 * 1024 * 1024 * 1024L / 100; // 4.75TB
        private readonly RavenLogger _logger;

        public string RemoteFolderName { get; }
        public Sparrow.Size MaxUploadPutBlob { get; set; } = new Sparrow.Size(256, SizeUnit.Megabytes);
        public Sparrow.Size MaxSingleBlockSize { get; set; } = new Sparrow.Size(100, SizeUnit.Megabytes);

        public LegacyRavenAzureClient(AzureSettings azureSettings, Progress progress = null, RavenLogger logger = null, CancellationToken? cancellationToken = null)
            : base(progress, cancellationToken)
        {
            var hasAccountKey = string.IsNullOrWhiteSpace(azureSettings.AccountKey) == false;
            _hasSasToken = string.IsNullOrWhiteSpace(azureSettings.SasToken) == false;

            if (hasAccountKey == false && _hasSasToken == false)
            {
                throw new ArgumentException($"{nameof(AzureSettings.AccountKey)} and {nameof(AzureSettings.SasToken)} cannot be both null or empty");
            }

            if (hasAccountKey && _hasSasToken)
            {
                throw new ArgumentException($"{nameof(AzureSettings.AccountKey)} and {nameof(AzureSettings.SasToken)} cannot be used simultaneously");
            }

            if (string.IsNullOrWhiteSpace(azureSettings.AccountName))
                throw new ArgumentException($"{nameof(AzureSettings.AccountName)} cannot be null or empty");

            if (string.IsNullOrWhiteSpace(azureSettings.StorageContainer))
                throw new ArgumentException($"{nameof(AzureSettings.StorageContainer)} cannot be null or empty");


            if (hasAccountKey)
            {
                _accountKey = GetAccountKeyBytes(azureSettings.AccountKey);
            }
            else
            {
                VerifySasToken(azureSettings.SasToken);
                _sasToken = azureSettings.SasToken;
            }

            RemoteFolderName = azureSettings.RemoteFolderName;

            _accountName = azureSettings.AccountName;
            _containerName = azureSettings.StorageContainer;

            _serverUrlForContainer = $"https://{_accountName}.blob.core.windows.net/{_containerName.ToLower()}";
            _serverUrlForAccountName = $"https://{_accountName}.blob.core.windows.net";
            _logger = logger;
        }

        private static byte[] GetAccountKeyBytes(string accountKey)
        {
            try
            {
                return Convert.FromBase64String(accountKey);
            }
            catch (Exception e)
            {
                throw new ArgumentException($"Wrong format for {nameof(AzureSettings.AccountKey)}", e);
            }
        }

        private static void VerifySasToken(string sasToken)
        {
            try
            {
                var splitted = sasToken.Split('&');
                if (splitted.Length == 0)
                    throw new ArgumentException($"{nameof(AzureSettings.SasToken)} isn't in the correct format");

                foreach (var keyValueString in splitted)
                {
                    var keyValue = keyValueString.Split('=');
                    if (string.IsNullOrEmpty(keyValue[0]) || string.IsNullOrEmpty(keyValue[1]))
                        throw new ArgumentException($"{nameof(AzureSettings.SasToken)} isn't in the correct format");
                }
            }
            catch (Exception e)
            {
                throw new ArgumentException($"{nameof(AzureSettings.SasToken)} isn't in the correct format", e);
            }
        }

        private string GetUrl(string baseUrl, string parameters = null)
        {
            var url = baseUrl;

            var hasParameters = string.IsNullOrEmpty(parameters) == false;
            if (hasParameters)
                url += $"?{parameters}";

            if (_hasSasToken)
            {
                if (hasParameters)
                {
                    url += "&";
                }
                else
                {
                    url += "?";
                }

                url += _sasToken;
            }

            return url;
        }

        public void PutBlob(string key, Stream stream, Dictionary<string, string> metadata)
        {
            AsyncHelpers.RunSync(TestConnectionAsync);

            if (stream.Length > MaxUploadPutBlob.GetValue(SizeUnit.Bytes))
            {
                // for blobs over 256MB
                PutBlockApi(key, stream, metadata);
                return;
            }

            var url = GetUrl($"{_serverUrlForContainer}/{key}");

            Progress?.UploadProgress.SetTotal(stream.Length);

            var now = SystemTime.UtcNow;
            // stream is disposed by the HttpClient
            var content = new ProgressableStreamContent(stream, Progress)
            {
                Headers =
                {
                    {"x-ms-date", now.ToString("R")},
                    {"x-ms-version", AzureStorageVersion},
                    {"x-ms-blob-type", "BlockBlob"},
                    {Constants.Headers.ContentLength, stream.Length.ToString(CultureInfo.InvariantCulture)}
                }
            };

            foreach (var metadataKey in metadata.Keys)
                content.Headers.Add("x-ms-meta-" + metadataKey.ToLower(), metadata[metadataKey]);

            var client = GetClient(TimeSpan.FromHours(3));
            SetAuthorizationHeader(client, HttpMethods.Put, url, content.Headers);

            var response = client.PutAsync(url, content, CancellationToken).Result;
            Progress?.UploadProgress.ChangeState(UploadState.Done);
            if (response.IsSuccessStatusCode)
                return;

            throw StorageException.FromResponseMessage(response);
        }

        private void PutBlockApi(string key, Stream stream, Dictionary<string, string> metadata)
        {
            var streamLength = stream.Length;
            if (streamLength > TotalBlocksSizeLimitInBytes)
                throw new InvalidOperationException(@"Can't upload more than 4.75TB to Azure, " +
                                                    $"current upload size: {new Size(streamLength).HumaneSize}");

            var blockNumber = 0;
            var blockIds = new List<string>();
            var baseUrl = _serverUrlForContainer + "/" + key;
            var client = GetClient(TimeSpan.FromDays(7));

            Progress?.UploadProgress.SetTotal(streamLength);
            Progress?.UploadProgress.ChangeType(UploadType.Chunked);

            try
            {
                var maxSingleBlockSize = MaxSingleBlockSize.GetValue(SizeUnit.Bytes);

                while (stream.Position < streamLength)
                {
                    var blockNumberInBytes = BitConverter.GetBytes(blockNumber++);
                    var blockIdString = Convert.ToBase64String(blockNumberInBytes);
                    blockIds.Add(blockIdString);

                    var length = Math.Min(maxSingleBlockSize, streamLength - stream.Position);
                    var parameters = $"comp=block&blockid={WebUtility.UrlEncode(blockIdString)}";
                    var url = GetUrl(baseUrl, parameters);

                    PutBlock(stream, client, url, length, retryCount: 0);
                }

                // put block list
                PutBlockList(baseUrl, client, blockIds, metadata);
            }
            finally
            {
                Progress?.UploadProgress.ChangeState(UploadState.Done);
            }
        }

        private void PutBlock(Stream baseStream, RavenHttpClient client, string url, long length, int retryCount)
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
                        {"x-ms-date", now.ToString("R")},
                        {"x-ms-version", AzureStorageVersion},
                        {Constants.Headers.ContentLength, subStream.Length.ToString(CultureInfo.InvariantCulture)}
                    }
                };

                SetAuthorizationHeader(client, HttpMethods.Put, url, content.Headers);

                try
                {
                    var response = client.PutAsync(url, content, CancellationToken).Result;
                    if (response.IsSuccessStatusCode)
                        return;

                    if (response.StatusCode == HttpStatusCode.RequestEntityTooLarge ||
                        response.StatusCode == HttpStatusCode.Conflict ||
                        response.StatusCode == HttpStatusCode.BadRequest)
                        throw StorageException.FromResponseMessage(response);

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
                _logger.Info($"Trying to send the request again. Retries count: '{retryCount}', Container: '{_containerName}'.");

            // restore the stream position before retrying
            baseStream.Position = position;
            PutBlock(baseStream, client, url, length, retryCount);
        }

        private void PutBlockList(string baseUrl, RavenHttpClient client, List<string> blockIds, Dictionary<string, string> metadata)
        {
            var url = GetUrl(baseUrl, "comp=blocklist");
            var now = SystemTime.UtcNow;
            var doc = CreateXmlDocument(blockIds);
            var xmlString = doc.OuterXml;

            var content = new StringContent(xmlString, Encoding.UTF8, "text/plain")
            {
                Headers =
                {
                    {"x-ms-date", now.ToString("R")},
                    {"x-ms-version", AzureStorageVersion},
                    {Constants.Headers.ContentLength, Encoding.UTF8.GetBytes(xmlString).Length.ToString(CultureInfo.InvariantCulture)}
                }
            };

            foreach (var metadataKey in metadata.Keys)
                content.Headers.Add("x-ms-meta-" + metadataKey.ToLower(), metadata[metadataKey]);

            SetAuthorizationHeader(client, HttpMethods.Put, url, content.Headers);

            var response = client.PutAsync(url, content, CancellationToken).Result;
            if (response.IsSuccessStatusCode)
                return;

            throw StorageException.FromResponseMessage(response);
        }

        public async Task TestConnectionAsync()
        {
            try
            {
                if (await ContainerExistsAsync() == false)
                    throw new ContainerNotFoundException($"Container '{_containerName}' wasn't found!");
            }
            catch (UnauthorizedAccessException)
            {
                // we don't have the permissions to see if the container exists
            }
        }

        private async Task<bool> ContainerExistsAsync()
        {
            var url = GetUrl(_serverUrlForContainer, "restype=container");
            var now = SystemTime.UtcNow;
            var requestMessage = new HttpRequestMessage(HttpMethods.Get, url)
            {
                Headers =
                {
                    {"x-ms-date", now.ToString("R")},
                    {"x-ms-version", AzureStorageVersion}
                }
            };

            var client = GetClient();
            SetAuthorizationHeader(client, HttpMethods.Get, url, requestMessage.Headers);

            var response = await client.SendAsync(requestMessage, CancellationToken);
            if (response.IsSuccessStatusCode)
                return true;

            if (response.StatusCode == HttpStatusCode.NotFound)
                return false;

            var error = StorageException.FromResponseMessage(response);

            if (response.StatusCode == HttpStatusCode.Forbidden)
                throw new UnauthorizedAccessException(error.ResponseString);

            throw error;
        }

        private static XmlDocument CreateXmlDocument(List<string> blockIds)
        {
            var doc = new XmlDocument();
            var xmlDeclaration = doc.CreateXmlDeclaration("1.0", "UTF-8", null);
            var root = doc.DocumentElement;
            doc.InsertBefore(xmlDeclaration, root);

            var blockList = doc.CreateElement("BlockList");
            doc.AppendChild(blockList);
            foreach (var blockId in blockIds)
            {
                var uncommitted = doc.CreateElement("Uncommitted");
                var text = doc.CreateTextNode(blockId);
                uncommitted.AppendChild(text);
                blockList.AppendChild(uncommitted);
            }
            return doc;
        }

        public void PutContainer()
        {
            var url = GetUrl(_serverUrlForContainer, "restype=container");

            var now = SystemTime.UtcNow;
            var content = new EmptyContent
            {
                Headers =
                {
                    {"x-ms-date", now.ToString("R")},
                    {"x-ms-version", AzureStorageVersion}
                }
            };

            var client = GetClient();
            SetAuthorizationHeader(client, HttpMethods.Put, url, content.Headers);

            var response = client.PutAsync(url, content, CancellationToken).Result;
            if (response.IsSuccessStatusCode)
                return;

            if (response.StatusCode == HttpStatusCode.Conflict)
                return;

            throw StorageException.FromResponseMessage(response);
        }

        public async Task<Blob> GetBlobAsync(string key)
        {
            var url = GetUrl($"{_serverUrlForContainer}/{key}");

            var now = SystemTime.UtcNow;

            var requestMessage = new HttpRequestMessage(HttpMethods.Get, url)
            {
                Headers =
                {
                    {"x-ms-date", now.ToString("R")},
                    {"x-ms-version", AzureStorageVersion}
                }
            };

            var client = GetClient();
            SetAuthorizationHeader(client, HttpMethods.Get, url, requestMessage.Headers);

            var response = await client.SendAsync(requestMessage, HttpCompletionOption.ResponseHeadersRead, CancellationToken);
            if (response.StatusCode == HttpStatusCode.NotFound)
                return null;

            if (response.IsSuccessStatusCode == false)
                throw StorageException.FromResponseMessage(response);

            var data = await response.Content.ReadAsStreamAsync();
            var headers = response.Headers.ToDictionary(x => x.Key, x => x.Value.FirstOrDefault());

            if (response.Content.Headers.TryGetValues(Constants.Headers.ContentLength, out var values) == false)
                throw new InvalidOperationException("Content-Length header is not present");

            var contentLength = values.FirstOrDefault();
            if (long.TryParse(contentLength, out var size) == false)
                throw new InvalidOperationException($"Content-Length header is present but could not be parsed, got: {contentLength}");

            return new Blob(data, headers, size);
        }

        public void DeleteContainer()
        {
            var url = GetUrl(_serverUrlForContainer, "restype=container");

            var now = SystemTime.UtcNow;
            var requestMessage = new HttpRequestMessage(HttpMethods.Delete, url)
            {
                Headers =
                {
                    {"x-ms-date", now.ToString("R")},
                    {"x-ms-version", AzureStorageVersion}
                }
            };

            var client = GetClient();
            SetAuthorizationHeader(client, HttpMethods.Delete, url, requestMessage.Headers);

            var response = client.SendAsync(requestMessage, CancellationToken).Result;
            if (response.IsSuccessStatusCode)
                return;

            if (response.StatusCode == HttpStatusCode.NotFound)
                return;

            throw StorageException.FromResponseMessage(response);
        }

        public ListBlobResult ListBlobs(string prefix, string delimiter, bool listFolders, string marker = null)
        {
            return ListBlobsAsync(prefix, delimiter, listFolders, marker).Result;
        }

        public async Task<ListBlobResult> ListBlobsAsync(string prefix, string delimiter, bool listFolders, string marker = null)
        {
            var url = GetUrl(_serverUrlForContainer, "restype=container&comp=list");
            if (prefix != null)
                url += $"&prefix={Uri.EscapeDataString(prefix)}";

            if (delimiter != null)
                url += $"&delimiter={delimiter}";

            //if (maxResult != null)
            //    url += $"&maxresults={maxResult}";

            if (marker != null)
                url += $"&marker={marker}";

            var requestMessage = new HttpRequestMessage(HttpMethods.Get, url)
            {
                Headers =
                {
                    {"x-ms-date", SystemTime.UtcNow.ToString("R")},
                    {"x-ms-version", AzureStorageVersion}
                }
            };
            var client = GetClient();
            SetAuthorizationHeader(client, HttpMethods.Get, url, requestMessage.Headers);

            var response = await client.SendAsync(requestMessage, CancellationToken).ConfigureAwait(false);
            if (response.StatusCode == HttpStatusCode.NotFound)
                return new ListBlobResult
                {
                    List = new List<BlobProperties>()
                };

            if (response.IsSuccessStatusCode == false)
                throw StorageException.FromResponseMessage(response);

            var responseStream = await response.Content.ReadAsStreamAsync();
            var listBlobsResult = XDocument.Load(responseStream);
            var result = GetResult();

            var nextMarker = listBlobsResult.Root.Element("NextMarker")?.Value;

            return new ListBlobResult
            {
                List = result,
                ContinuationToken = nextMarker == "true" ? listBlobsResult.Root.Element("NextMarker")?.Value : null
            };

            IEnumerable<BlobProperties> GetResult()
            {
                if (listFolders)
                {
                    return listBlobsResult
                        .Descendants("Blobs")
                        .Descendants("Name")
                        .Select(x => RestorePointsBase.GetDirectoryName(x.Value))
                        .Distinct()
                        .Select(x => new BlobProperties
                        {
                            Name = x
                        });
                }

                return listBlobsResult
                    .Descendants("Blob")
                    .Select(x => new BlobProperties
                    {
                        Name = x.Element("Name")?.Value,
                    });
            }
        }

        public void DeleteBlobs(List<string> blobs)
        {
            if (blobs.Count == 0)
                return;

            var client = GetClient();

            foreach (var blob in blobs)
            {
                var url = GetUrl($"{_serverUrlForContainer}/{blob}");

                var now = SystemTime.UtcNow;

                var requestMessage = new HttpRequestMessage(HttpMethods.Delete, url)
                {
                    Headers =
                    {
                        {"x-ms-date", now.ToString("R")},
                        {"x-ms-version", AzureStorageVersion}
                    }
                };

                SetAuthorizationHeader(client, HttpMethods.Delete, url, requestMessage.Headers);
                var response = client.SendAsync(requestMessage, CancellationToken).Result;
                if (response.IsSuccessStatusCode)
                    continue;

                if (response.StatusCode == HttpStatusCode.NotFound)
                    continue;

                throw StorageException.FromResponseMessage(response);
            }
        }

        public List<string> GetContainerNames(int maxResults)
        {
            var url = GetUrl(_serverUrlForAccountName, $"comp=list&maxresults={maxResults}");

            var now = SystemTime.UtcNow;

            var requestMessage = new HttpRequestMessage(HttpMethods.Get, url)
            {
                Headers =
                {
                    {"x-ms-date", now.ToString("R")},
                    {"x-ms-version", AzureStorageVersion}
                }
            };

            var client = GetClient();
            SetAuthorizationHeader(client, HttpMethods.Get, url, requestMessage.Headers);

            var response = client.SendAsync(requestMessage, CancellationToken).Result;
            if (response.IsSuccessStatusCode == false)
                throw StorageException.FromResponseMessage(response);

            var containersList = new List<string>();

            using (var stream = response.Content.ReadAsStreamAsync().Result)
            using (var reader = new StreamReader(stream))
            {
                var xDocument = XDocument.Load(reader);

                foreach (var node in xDocument.Descendants("Containers"))
                {
                    foreach (var containerNode in node.Descendants("Container"))
                    {
                        var nodeName = containerNode.Element("Name").Value;
                        containersList.Add(nodeName);
                    }
                }
            }

            return containersList;
        }

        private void SetAuthorizationHeader(RavenHttpClient httpClient, HttpMethod httpMethod, string url, HttpHeaders httpHeaders, HttpHeaders httpContentHeaders = null)
        {
            if (_hasSasToken)
            {
                // we pass the sas token in the url
                return;
            }

            var stringToHash = ComputeCanonicalizedHeaders(httpMethod, httpHeaders, httpContentHeaders);
            stringToHash += ComputeCanonicalizedResource(url);

            if (stringToHash.EndsWith("\n"))
                stringToHash = stringToHash.Substring(0, stringToHash.Length - 1);

            using (var hash = new HMACSHA256(_accountKey))
            {
                var hashedString = hash.ComputeHash(Encoding.UTF8.GetBytes(stringToHash));
                var base64String = Convert.ToBase64String(hashedString);

                httpClient.DefaultRequestHeaders.Authorization =
                    new AuthenticationHeaderValue("SharedKey", $"{_accountName}:{base64String}");
            }
        }

        private static string ComputeCanonicalizedHeaders(HttpMethod httpMethod, HttpHeaders httpHeaders, HttpHeaders httpContentHeaders = null)
        {
            var contentLength = string.Empty;
            var contentType = string.Empty;

            var headers = httpHeaders.Where(x => x.Key.StartsWith("x-ms-")).OrderBy(x => x.Key);

            if (httpContentHeaders != null)
            {
                if (httpContentHeaders.TryGetValues(Constants.Headers.ContentLength, out IEnumerable<string> lengthValues))
                    contentLength = lengthValues.First();

                if (httpContentHeaders.TryGetValues(Constants.Headers.ContentType, out IEnumerable<string> typeValues))
                    contentType = typeValues.First();
            }
            else
            {
                if (httpHeaders.TryGetValues(Constants.Headers.ContentLength, out IEnumerable<string> lengthValues))
                    contentLength = lengthValues.First();

                if (httpHeaders.TryGetValues(Constants.Headers.ContentType, out IEnumerable<string> typeValues))
                    contentType = typeValues.First();
            }

            var stringToHash = $"{httpMethod.Method}\n\n\n{contentLength}\n\n{contentType}\n\n\n\n\n\n\n";

            return headers.Aggregate(stringToHash, (current, header) => current + $"{header.Key.ToLower()}:{header.Value.First()}\n");
        }

        private string ComputeCanonicalizedResource(string url)
        {
            var uri = new Uri(url, UriKind.Absolute);

            var stringToHash = $"/{_accountName}{uri.AbsolutePath}\n";
            return QueryHelpers.ParseQuery(uri.Query)
                .OrderBy(x => x.Key)
                .Aggregate(stringToHash, (current, parameter) => current + $"{parameter.Key.ToLower()}:{parameter.Value}\n");
        }

        private sealed class EmptyContent : HttpContent
        {
            protected override Task SerializeToStreamAsync(Stream stream, TransportContext context)
            {
                return Task.CompletedTask;
            }

            protected override bool TryComputeLength(out long length)
            {
                length = 0;
                return true;
            }
        }

        public IMultiPartUploader GetUploader(string key, Dictionary<string, string> metadata)
        {
            throw new NotSupportedException("Multi part uploader isn't supported for the legacy azure client");
        }
    }
}
