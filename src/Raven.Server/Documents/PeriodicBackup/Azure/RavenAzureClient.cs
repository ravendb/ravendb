// -----------------------------------------------------------------------
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
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Util;
using Raven.Server.Exceptions.PeriodicBackup;
using Sparrow.Logging;

namespace Raven.Server.Documents.PeriodicBackup.Azure
{
    public class RavenAzureClient : RavenStorageClient
    {
        private readonly string _accountName;
        private readonly byte[] _accountKey;
        private readonly string _containerName;
        private readonly string _serverUrlForContainer;
        private const string AzureStorageVersion = "2017-04-17";
        private const int MaxUploadPutBlobInBytes = 256 * 1024 * 1024; // 256MB
        private const int OnePutBlockSizeLimitInBytes = 100 * 1024 * 1024; // 100MB
        private const long TotalBlocksSizeLimitInBytes = 475L * 1024 * 1024 * 1024 * 1024L / 100; // 4.75TB
        private readonly Logger _logger;
        public static bool TestMode;

        public RavenAzureClient(AzureSettings azureSettings, Progress progress = null, Logger logger = null, CancellationToken? cancellationToken = null)
            : base(progress, cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(azureSettings.AccountKey))
                throw new ArgumentException("Account Key cannot be null or empty");

            if (string.IsNullOrWhiteSpace(azureSettings.AccountName))
                throw new ArgumentException("Account Name cannot be null or empty");

            if (string.IsNullOrWhiteSpace(azureSettings.StorageContainer))
                throw new ArgumentException("Storage Container cannot be null or empty");

            _accountName = azureSettings.AccountName;

            try
            {
                _accountKey = Convert.FromBase64String(azureSettings.AccountKey);
            }
            catch (Exception e)
            {
                throw new ArgumentException("Wrong format for Account Key", e);
            }

            _containerName = azureSettings.StorageContainer;

            _logger = logger;
            _serverUrlForContainer = GetUrlForContainer(azureSettings.StorageContainer.ToLower());
        }

        private string GetUrlForContainer(string containerName)
        {
            var template = TestMode == false ? "https://{0}.blob.core.windows.net/{1}" : "http://localhost:10000/{0}/{1}";
            return string.Format(template, _accountName, containerName);
        }

        private string GetBaseServerUrl()
        {
            var template = TestMode == false ? "https://{0}.blob.core.windows.net" : "http://localhost:10000/{0}";
            return string.Format(template, _accountName);
        }

        public void PutBlob(string key, Stream stream, Dictionary<string, string> metadata)
        {
            TestConnection();

            if (stream.Length > MaxUploadPutBlobInBytes)
            {
                // for blobs over 256MB
                PutBlockApi(key, stream, metadata);
                return;
            }

            var url = _serverUrlForContainer + "/" + key;

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
                    {"Content-Length", stream.Length.ToString(CultureInfo.InvariantCulture)}
                }
            };

            foreach (var metadataKey in metadata.Keys)
                content.Headers.Add("x-ms-meta-" + metadataKey.ToLower(), metadata[metadataKey]);

            var client = GetClient(TimeSpan.FromHours(3));
            client.DefaultRequestHeaders.Authorization = CalculateAuthorizationHeaderValue(HttpMethods.Put, url, content.Headers);

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
                while (stream.Position < streamLength)
                {
                    var blockNumberInBytes = BitConverter.GetBytes(blockNumber++);
                    var blockIdString = Convert.ToBase64String(blockNumberInBytes);
                    blockIds.Add(blockIdString);

                    var length = Math.Min(OnePutBlockSizeLimitInBytes, streamLength - stream.Position);
                    var baseUrlForUpload = baseUrl + "?comp=block&blockid=";
                    var url = baseUrlForUpload + WebUtility.UrlEncode(blockIdString);

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

        private void PutBlock(Stream baseStream, HttpClient client, string url, long length, int retryCount)
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
                        {"Content-Length", subStream.Length.ToString(CultureInfo.InvariantCulture)}
                    }
                };

                client.DefaultRequestHeaders.Authorization = CalculateAuthorizationHeaderValue(HttpMethods.Put, url, content.Headers);

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

        private void PutBlockList(string baseUrl, HttpClient client,
            List<string> blockIds, Dictionary<string, string> metadata)
        {
            var url = baseUrl + "?comp=blocklist";
            var now = SystemTime.UtcNow;
            var doc = CreateXmlDocument(blockIds);
            var xmlString = doc.OuterXml;

            var content = new StringContent(xmlString, Encoding.UTF8, "text/plain")
            {
                Headers =
                {
                    {"x-ms-date", now.ToString("R")},
                    {"x-ms-version", AzureStorageVersion},
                    {"Content-Length", Encoding.UTF8.GetBytes(xmlString).Length.ToString(CultureInfo.InvariantCulture)}
                }
            };

            foreach (var metadataKey in metadata.Keys)
                content.Headers.Add("x-ms-meta-" + metadataKey.ToLower(), metadata[metadataKey]);

            client.DefaultRequestHeaders.Authorization = CalculateAuthorizationHeaderValue(HttpMethods.Put, url, content.Headers);

            var response = client.PutAsync(url, content, CancellationToken).Result;
            if (response.IsSuccessStatusCode)
                return;

            throw StorageException.FromResponseMessage(response);
        }

        public void TestConnection()
        {
            if (ContainerExists())
                return;

            throw new ContainerNotFoundException($"Container '{_containerName}' not found!");
        }

        private bool ContainerExists()
        {
            var url = _serverUrlForContainer + "?restype=container";
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
            client.DefaultRequestHeaders.Authorization = CalculateAuthorizationHeaderValue(HttpMethods.Get, url, requestMessage.Headers);

            var response = client.SendAsync(requestMessage, CancellationToken).Result;
            if (response.IsSuccessStatusCode)
                return true;

            if (response.StatusCode == HttpStatusCode.NotFound)
                return false;

            throw StorageException.FromResponseMessage(response);
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
            var url = _serverUrlForContainer + "?restype=container";

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
            client.DefaultRequestHeaders.Authorization = CalculateAuthorizationHeaderValue(HttpMethods.Put, url, content.Headers);

            var response = client.PutAsync(url, content, CancellationToken).Result;
            if (response.IsSuccessStatusCode)
                return;

            if (response.StatusCode == HttpStatusCode.Conflict)
                return;

            throw StorageException.FromResponseMessage(response);
        }

        public Blob GetBlob(string key)
        {
            var url = _serverUrlForContainer + "/" + key;

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
            client.DefaultRequestHeaders.Authorization = CalculateAuthorizationHeaderValue(HttpMethods.Get, url, requestMessage.Headers);

            var response = client.SendAsync(requestMessage, HttpCompletionOption.ResponseHeadersRead, CancellationToken).Result;
            if (response.StatusCode == HttpStatusCode.NotFound)
                return null;

            if (response.IsSuccessStatusCode == false)
                throw StorageException.FromResponseMessage(response);

            var data = response.Content.ReadAsStreamAsync().Result;
            var headers = response.Headers.ToDictionary(x => x.Key, x => x.Value.FirstOrDefault());

            return new Blob(data, headers);
        }
        public async Task<Blob> GetBlobAsync(string key)
        {
            var url = _serverUrlForContainer + "/" + key;

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
            client.DefaultRequestHeaders.Authorization = CalculateAuthorizationHeaderValue(HttpMethods.Get, url, requestMessage.Headers);

            var response = await client.SendAsync(requestMessage, HttpCompletionOption.ResponseHeadersRead, CancellationToken);
            if (response.StatusCode == HttpStatusCode.NotFound)
                return null;

            if (response.IsSuccessStatusCode == false)
                throw StorageException.FromResponseMessage(response);

            var data = await response.Content.ReadAsStreamAsync();
            var headers = response.Headers.ToDictionary(x => x.Key, x => x.Value.FirstOrDefault());

            return new Blob(data, headers);
        }

        public void DeleteContainer()
        {
            var url = _serverUrlForContainer + "?restype=container";

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
            client.DefaultRequestHeaders.Authorization = CalculateAuthorizationHeaderValue(HttpMethods.Delete, url, requestMessage.Headers);

            var response = client.SendAsync(requestMessage, CancellationToken).Result;
            if (response.IsSuccessStatusCode)
                return;

            if (response.StatusCode == HttpStatusCode.NotFound)
                return;

            throw StorageException.FromResponseMessage(response);
        }

        public async Task<ListBlobResult> ListBlobs(string prefix, string delimiter, bool listFolders, int? maxResult = null, string marker = null)
        {
            var url = GetBaseServerUrl() + $"/{_containerName}?restype=container&comp=list";
            if (prefix != null)
                url += $"&prefix={Uri.EscapeDataString(prefix)}";

            if (delimiter != null)
                url += $"&delimiter={delimiter}";

            if (maxResult != null)
                url += $"&maxresults={maxResult}";

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
            client.DefaultRequestHeaders.Authorization = CalculateAuthorizationHeaderValue(HttpMethods.Get, url, requestMessage.Headers);

            var response = await client.SendAsync(requestMessage, CancellationToken).ConfigureAwait(false);
            if (response.StatusCode == HttpStatusCode.NotFound)
                return new ListBlobResult
                {
                    ListBlob = new List<BlobProperties>()
                };

            if (response.IsSuccessStatusCode == false)
                throw StorageException.FromResponseMessage(response);

            var responseStream = await response.Content.ReadAsStreamAsync();
            var listBlobsResult = XDocument.Load(responseStream);
            var result = GetResult();

            var nextMarker = listBlobsResult.Root.Element("NextMarker")?.Value;

            return new ListBlobResult
            {
                ListBlob = result,
                NextMarker = nextMarker == "true" ? listBlobsResult.Root.Element("NextMarker")?.Value : null
            };

            List<BlobProperties> GetResult()
            {
                if (listFolders)
                {
                    return listBlobsResult
                        .Descendants("Blobs")
                        .Descendants("Name")
                        .Select(x => GetDirectoryName(x.Value))
                        .Distinct()
                        .Select(x => new BlobProperties
                        {
                            Name = x
                        })
                        .ToList();
                }
                else
                {
                    return listBlobsResult
                        .Descendants("Blob")
                        .Select(x => new BlobProperties
                        {
                            Name = x.Element("Name")?.Value,
                            LastModified = Convert.ToDateTime(x.Element("Properties")?.Element("Last-Modified")?.Value)
                        })
                        .ToList();
                }
            }
        }

        public List<string> GetContainerNames(int maxResults)
        {
            var url = GetBaseServerUrl() + $"?comp=list&maxresults={maxResults}";

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
            client.DefaultRequestHeaders.Authorization = CalculateAuthorizationHeaderValue(HttpMethods.Get, url, requestMessage.Headers);

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

        private AuthenticationHeaderValue CalculateAuthorizationHeaderValue(
            HttpMethod httpMethod, string url, HttpHeaders httpHeaders)
        {
            var stringToHash = ComputeCanonicalizedHeaders(httpMethod, httpHeaders);
            stringToHash += ComputeCanonicalizedResource(url);

            if (stringToHash.EndsWith("\n"))
                stringToHash = stringToHash.Substring(0, stringToHash.Length - 1);

            using (var hash = new HMACSHA256(_accountKey))
            {
                var hashedString = hash.ComputeHash(Encoding.UTF8.GetBytes(stringToHash));
                var base64String = Convert.ToBase64String(hashedString);

                return new AuthenticationHeaderValue("SharedKey", $"{_accountName}:{base64String}");
            }
        }

        private static string ComputeCanonicalizedHeaders(HttpMethod httpMethod, HttpHeaders httpHeaders)
        {
            var headers = httpHeaders
                .Where(x => x.Key.StartsWith("x-ms-"))
                .OrderBy(x => x.Key);

            var contentLength = string.Empty;
            if (httpHeaders.TryGetValues("Content-Length", out IEnumerable<string> values))
                contentLength = values.First();

            var contentType = string.Empty;
            if (httpHeaders.TryGetValues("Content-Type", out values))
                contentType = values.First();

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

        private class EmptyContent : HttpContent
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
    }
}
