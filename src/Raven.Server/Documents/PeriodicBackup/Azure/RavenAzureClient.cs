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
using Raven.Server.Documents.PeriodicBackup.Restore;
using Raven.Server.Exceptions.PeriodicBackup;
using Sparrow.Logging;

namespace Raven.Server.Documents.PeriodicBackup.Azure
{
    public class RavenAzureClient : RavenStorageClient
    {
        private readonly bool _hasSasToken;
        private readonly string _accountName;
        private readonly byte[] _accountKey;
        private readonly string _sasToken;
        private readonly string _containerName;
        private readonly string _serverUrlForContainer;
        private readonly string _serverUrlForAccountName;
        private const string AzureStorageVersion = "2019-02-02";
        private const int MaxUploadPutBlobInBytes = 256 * 1024 * 1024; // 256MB
        private const int OnePutBlockSizeLimitInBytes = 100 * 1024 * 1024; // 100MB
        private const long TotalBlocksSizeLimitInBytes = 475L * 1024 * 1024 * 1024 * 1024L / 100; // 4.75TB
        private readonly Logger _logger;

        public static bool TestMode;


        public string RemoteFolderName { get; }

        public RavenAzureClient(AzureSettings azureSettings, Progress progress = null, Logger logger = null, CancellationToken? cancellationToken = null)
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

            _logger = logger;
            _serverUrlForContainer = GetUrlForContainer();
            _serverUrlForAccountName = GetUrlForAccountName();
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

        private string GetUrlForContainer()
        {
            var template = TestMode == false ? "https://{0}.blob.core.windows.net/{1}" : "http://localhost:10000/{0}/{1}";
            return string.Format(template, _accountName, _containerName.ToLower());
        }

        private string GetUrlForAccountName()
        {
            var template = TestMode == false ? "https://{0}.blob.core.windows.net" : "http://localhost:10000/{0}";
            return string.Format(template, _accountName);
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
            TestConnection();

            if (stream.Length > MaxUploadPutBlobInBytes)
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
                    {"Content-Length", stream.Length.ToString(CultureInfo.InvariantCulture)}
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
                while (stream.Position < streamLength)
                {
                    var blockNumberInBytes = BitConverter.GetBytes(blockNumber++);
                    var blockIdString = Convert.ToBase64String(blockNumberInBytes);
                    blockIds.Add(blockIdString);

                    var length = Math.Min(OnePutBlockSizeLimitInBytes, streamLength - stream.Position);
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

        private void PutBlockList(string baseUrl, HttpClient client, List<string> blockIds, Dictionary<string, string> metadata)
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
                    {"Content-Length", Encoding.UTF8.GetBytes(xmlString).Length.ToString(CultureInfo.InvariantCulture)}
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

        public void TestConnection()
        {
            try
            {
                if (ContainerExists() == false)
                    throw new ContainerNotFoundException($"Container '{_containerName}' wasn't found!");
            }
            catch (UnauthorizedAccessException)
            {
                // we don't have the permissions to see if the container exists
            }
        }

        private bool ContainerExists()
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

            var response = client.SendAsync(requestMessage, CancellationToken).Result;
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

        public Blob GetBlob(string key)
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

            return new Blob(data, headers);
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

        public ListBlobResult ListBlobs(string prefix, string delimiter, bool listFolders, int? maxResult = null, string marker = null)
        {
            return ListBlobsAsync(prefix, delimiter, listFolders, maxResult, marker).Result;
        }

        public async Task<ListBlobResult> ListBlobsAsync(string prefix, string delimiter, bool listFolders, int? maxResult = null, string marker = null)
        {
            var url = GetUrl(_serverUrlForContainer, "restype=container&comp=list");
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
                NextMarker = nextMarker == "true" ? listBlobsResult.Root.Element("NextMarker")?.Value : null
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

        public void DeleteMultipleBlobs(List<string> blobs)
        {
            if (blobs.Count == 0)
                return;

            DeleteBlobsWithSasToken(blobs);

            //TODO: RavenDB-16264
            /*
            if (_hasSasToken)
            {
                // Multi-Delete isn't supported when using a SAS token
                // https://issues.hibernatingrhinos.com/issue/RavenDB-14936
                // https://github.com/Azure/azure-sdk-for-net/issues/11762
                DeleteBlobsWithSasToken(blobs);
                return;
            }

            const string xMsDate = "x-ms-date";
            const string xMsClientRequestId = "x-ms-client-request-id";
            const string xMsReturnClientRequestId = "x-ms-return-client-request-id";

            var now = SystemTime.UtcNow.ToString("R");
            var url = GetUrl(_serverUrlForAccountName, "comp=batch");

            var requestMessage = new HttpRequestMessage(HttpMethods.Post, url)
            {
                Headers =
                {
                    { xMsDate, now }, { "x-ms-version", AzureStorageVersion }
                }
            };

            var batchContent = new MultipartContent("mixed", $"batch_{Guid.NewGuid()}");
            requestMessage.Content = batchContent;

            var blobsWithIds = new Dictionary<string, string>();
            for (var i = 0; i < blobs.Count; i++)
            {
                using var ms = new MemoryStream();
                using var writer = new StreamWriter(ms);
                var clientRequestId = Guid.NewGuid().ToString();
                blobsWithIds[clientRequestId] = blobs[i];

                writer.WriteLine($"{HttpMethods.Delete} /{_containerName}/{blobs[i]} HTTP/1.1");
                writer.WriteLine($"{xMsDate}: {now}");
                writer.WriteLine($"{xMsClientRequestId}: {clientRequestId}");
                writer.WriteLine($"{xMsReturnClientRequestId}: true");

                using (var hash = new HMACSHA256(_accountKey))
                {
                    var uri = new Uri($"{_serverUrlForContainer}/{blobs[i]}", UriKind.Absolute);
                    var hashStr =
                        $"{HttpMethods.Delete}\n\n\n\n\n\n\n\n\n\n\n\n{xMsClientRequestId}:{clientRequestId}\n{xMsDate}:{now}\n{xMsReturnClientRequestId}:true\n/{_accountName}{uri.AbsolutePath}";
                    writer.WriteLine($"Authorization: SharedKey {_accountName}:{Convert.ToBase64String(hash.ComputeHash(Encoding.UTF8.GetBytes(hashStr)))}");
                }

                writer.WriteLine("Content-Length: 0");
                writer.Flush();

                batchContent.Add(new ByteArrayContent(ms.ToArray())
                {
                    Headers =
                    {
                        { "Content-Type", "application/http" },
                        { "Content-Transfer-Encoding", "binary" },
                        { "Content-ID", $"{i}" }
                    }
                });
            }

            var client = GetClient();

            if (batchContent.Headers.ContentLength.HasValue == false)
            {
                // we need the ContentLength to CalculateAuthorizationHeaderValue
                // the ContentLength is calculated on the fly, it gets added to Headers only when we try to access it.
                throw new ArgumentException($"{nameof(MultipartContent)} should have content length");
            }

            SetAuthorizationHeader(client, HttpMethods.Post, url, requestMessage.Headers, batchContent.Headers);

            var response = client.SendAsync(requestMessage).Result;
            if (response.IsSuccessStatusCode == false)
                throw StorageException.FromResponseMessage(response);

            using var stream = response.Content.ReadAsStreamAsync().Result;
            using var reader = new StreamReader(stream);

            const string statusCode = "StatusCode";
            const string status = "Status";
            var responseBoundary = $"--{response.Content.Headers.ContentType.Parameters.First().Value}";
            var result = new Dictionary<string, Dictionary<string, string>>();

            while (reader.Peek() >= 0)
            {
                var line = reader.ReadLine();

                // read batch response
                if (line != responseBoundary && line != $"{responseBoundary}--")
                    throw new InvalidDataException("Got invalid response from server.");

                while (string.IsNullOrEmpty(line) == false)
                {
                    line = reader.ReadLine();
                }

                line = reader.ReadLine();

                // read sub-response block
                if (string.IsNullOrEmpty(line))
                    break;

                string[] res = line.Split(" ");
                var responseDictionary = new Dictionary<string, string>
                {
                    { statusCode, res[1] },
                    { status, res[1] == "202" ? res[res.Length - 1] : string.Join(" ", res, 2, res.Length - 2) }
                };

                line = reader.ReadLine();

                while (string.IsNullOrEmpty(line) == false)
                {
                    var r = line.Split(": ");
                    responseDictionary[r.First()] = r.Last();
                    line = reader.ReadLine();

                }
                result[blobsWithIds[responseDictionary["x-ms-client-request-id"]]] = responseDictionary;

                if (responseDictionary.TryGetValue("x-ms-error-code", out _))
                {
                    // read the error message body
                    line = reader.ReadLine();
                    if (line.StartsWith("<?xml"))
                    {
                        while (line.EndsWith("</Error>") == false)
                        {
                            line = reader.ReadLine();
                        }
                    }
                    else
                    {
                        while (string.IsNullOrEmpty(line) == false)
                        {
                            line = reader.ReadLine();
                        }
                    }
                }
            }

            var errors = result.Keys.Where(key => result[key][status] != "Accepted").ToDictionary(key => key, key => result[key][status]);
            var canLog = _logger != null && _logger.IsInfoEnabled;

            if (errors.Count == 0)
            {
                if (canLog)
                    _logger.Info($"Successfully deleted {result.Count} blob{Pluralize(result.Count)} from container: {_containerName}.");

                return;
            }

            var reasons = errors.Values.Distinct().ToArray();
            var failedToDeleteReasons = reasons.Aggregate(string.Empty, (current, r) =>
                current + $"Reason: {r} Blobs ({errors.Count(x => x.Value == r)}): {string.Join(", ", errors.Where(x => x.Value == r).Select(y => y.Key))}. ");

            var message = $"Failed to delete {errors.Count} blob{Pluralize(errors.Count)} from container: {_containerName}. Successfully deleted {result.Count - errors.Count} blob{Pluralize(result.Count - errors.Count)}. {failedToDeleteReasons}";

            if (canLog)
            {
                _logger.Info(message);
            }

            string Pluralize(int num)
            {
                return num == 0 || num > 1 ? "s" : string.Empty;
            }

            throw new InvalidOperationException(message);*/
        }

        private void DeleteBlobsWithSasToken(List<string> blobs)
        {
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

                var client = GetClient();
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

        private void SetAuthorizationHeader(HttpClient httpClient, HttpMethod httpMethod, string url, HttpHeaders httpHeaders, HttpHeaders httpContentHeaders = null)
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
                if (httpContentHeaders.TryGetValues("Content-Length", out IEnumerable<string> lengthValues))
                    contentLength = lengthValues.First();

                if (httpContentHeaders.TryGetValues("Content-Type", out IEnumerable<string> typeValues))
                    contentType = typeValues.First();
            }
            else
            {
                if (httpHeaders.TryGetValues("Content-Length", out IEnumerable<string> lengthValues))
                    contentLength = lengthValues.First();

                if (httpHeaders.TryGetValues("Content-Type", out IEnumerable<string> typeValues))
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
