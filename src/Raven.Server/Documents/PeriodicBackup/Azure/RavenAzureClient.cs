// -----------------------------------------------------------------------
//  <copyright file="RavenAzureClient.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
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
using Microsoft.AspNetCore.WebUtilities;
using Raven.Client.Util;
using Raven.Server.Exceptions.PeriodicBackup;
using Sparrow.Utils;

namespace Raven.Server.Documents.PeriodicBackup.Azure
{
    public class RavenAzureClient : RavenStorageClient
    {
        private readonly string _accountName;
        private readonly byte[] _accountKey;
        private readonly string _serverUrlForContainer;
        private readonly string _baseServerUrl;
        private const string AzureStorageVersion = "2016-05-31";
        private const int MaxUploadPutBlobInBytes = 256*1024*1024; //256MB
        private const int OnePutBlockSizeLimitInBytes = 100*1024*1024; //100MB
        private const long TotalBlocksSizeLimitInBytes = 475L*1024*1024*1024*1024L/100; //4.75TB

        public RavenAzureClient(string accountName, string accountKey, string containerName, bool isTest = false)
        {
            _accountName = accountName;

            try
            {
                _accountKey = Convert.FromBase64String(accountKey);
            }
            catch (Exception e)
            {
                throw new ArgumentException("Wrong format for account key", e);
            }

            _serverUrlForContainer = GetUrlForContainer(containerName.ToLower(), isTest);
            _baseServerUrl = GetUrlForContainer(string.Empty, isTest);
        }

        private string GetUrlForContainer(string containerName, bool isTest = false)
        {
            var template = isTest == false ? "https://{0}.blob.core.windows.net/{1}" : "http://localhost:10000/{0}/{1}";
            return string.Format(template, _accountName, containerName);
        }

        public async Task PutContainer()
        {
            var url = _serverUrlForContainer + "?restype=container";

            var now = SystemTime.UtcNow;
            var content = new EmptyContent
            {
                Headers =
                {
                    {"x-ms-date", now.ToString("R")},
                    {"x-ms-version", AzureStorageVersion},
                }
            };

            var client = GetClient();
            client.DefaultRequestHeaders.Authorization = CalculateAuthorizationHeaderValue(HttpMethods.Put.Method, url, content.Headers);

            var response = await client.PutAsync(url, content);
            if (response.IsSuccessStatusCode)
                return;

            if (response.StatusCode == HttpStatusCode.Conflict)
                return;

            throw StorageException.FromResponseMessage(response);
        }

        public async Task PutBlob(string key, Stream stream, Dictionary<string, string> metadata)
        {
            if (stream.Length > MaxUploadPutBlobInBytes)
            {
                //for blobs over 256MB
                await PutBlockApi(key, stream, metadata);
                return;
            }

            var url = _serverUrlForContainer + "/" + key;

            var now = SystemTime.UtcNow;
            var content = new StreamContent(stream)
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

            var client = GetClient(TimeSpan.FromHours(1));
            client.DefaultRequestHeaders.Authorization = CalculateAuthorizationHeaderValue(HttpMethods.Put.Method, url, content.Headers);

            var response = await client.PutAsync(url, content);
            if (response.IsSuccessStatusCode)
                return;

            throw StorageException.FromResponseMessage(response);
        }

        private async Task PutBlockApi(string key, Stream stream, Dictionary<string, string> metadata)
        {
            if (stream.Length > TotalBlocksSizeLimitInBytes)
                throw new InvalidOperationException($"Can't upload more than 4.75TB to Azure, current upload size: {new Size(stream.Length).HumaneSize}");

            var threads = ProcessorInfo.ProcessorCount/2 + 1;
            //max size of in memory queue is: 500MB
            var queue = new BlockingCollection<ByteArrayWithBlockId>(Math.Min(5, threads*2));
            //List of block ids; the blocks will be committed in the order in this list
            var blockIds = new List<string>();

            using (var cts = new CancellationTokenSource())
            {
                var tasks = new Task[threads];
                var fillQueueTask = CreateFillQueueTask(stream, blockIds, queue, cts);
                tasks[0] = fillQueueTask;

                var baseUrl = _serverUrlForContainer + "/" + key;
                for (var i = 1; i < threads; i++)
                {
                    var baseUrlForUpload = baseUrl + "?comp=block&blockid=";
                    var task = CreateUploadTask(queue, baseUrlForUpload, cts);
                    tasks[i] = task;
                }

                try
                {
                    //wait for all tasks to complete
                    await Task.WhenAll(tasks);
                }
                catch (Exception)
                {
                    GetExceptionsFromTasks(tasks);
                }

                //put block list
                await PutBlockList(baseUrl, blockIds, metadata);
            }
        }

        private static void GetExceptionsFromTasks(Task[] tasks)
        {
            var exceptions = new List<Exception>();

            foreach (var task in tasks)
            {
                if (task.IsCanceled == false && task.IsFaulted == false)
                    continue;

                exceptions.Add(task.Exception);
            }

            if (exceptions.Count > 0)
                throw new AggregateException(exceptions);
        }

        private static Task CreateFillQueueTask(Stream inputStream, List<string> blockIds,
            BlockingCollection<ByteArrayWithBlockId> queue, CancellationTokenSource cts)
        {
            var fillQueueTask = Task.Run(() =>
            {
                // Put Block is limited to 100MB per block
                var buffer = new byte[OnePutBlockSizeLimitInBytes];

                try
                {
                    var blockNumber = 0;
                    while (true)
                    {
                        if (cts.IsCancellationRequested)
                            return;

                        var read = inputStream.Read(buffer, 0, buffer.Length);
                        if (read <= 0)
                            break;

                        while (read < buffer.Length)
                        {
                            var lastRead = inputStream.Read(buffer, read, buffer.Length - read);
                            if (lastRead <= 0)
                                break;
                            read += lastRead;
                        }

                        var destination = new byte[read];
                        Buffer.BlockCopy(buffer, 0, destination, 0, read);
                        var blockNumberInBytes = BitConverter.GetBytes(blockNumber++);
                        var blockIdString = Convert.ToBase64String(blockNumberInBytes);

                        blockIds.Add(blockIdString);
                        var byteArrayWithBlockId = new ByteArrayWithBlockId
                        {
                            StreamAsByteArray = destination,
                            BlockId = blockIdString
                        };

                        while (queue.TryAdd(byteArrayWithBlockId, millisecondsTimeout: 200) == false)
                        {
                            if (cts.IsCancellationRequested)
                                return;
                        }
                    }
                }
                catch (Exception)
                {
                    //if we can't read from the input stream,
                    //we need to cancel the upload tasks
                    cts.Cancel();
                    throw;
                }
                finally
                {
                    queue.CompleteAdding();
                    //we don't need the stream anymore
                    inputStream.Dispose();
                }
            });
            return fillQueueTask;
        }

        private Task CreateUploadTask(BlockingCollection<ByteArrayWithBlockId> queue,
            string baseUrl, CancellationTokenSource cts)
        {
            //open http client for each task
            //we have different authorization header for each request
            var client = GetClient(TimeSpan.FromHours(1));
            return Task.Run(async () =>
            {
                while (true)
                {
                    if (cts.IsCancellationRequested)
                        return;

                    ByteArrayWithBlockId byteArrayWithBlockId;
                    while (queue.TryTake(out byteArrayWithBlockId, millisecondsTimeout: 200) == false)
                    {
                        if (queue.IsAddingCompleted || cts.IsCancellationRequested)
                        {
                            //no more streams to upload
                            return;
                        }
                    }

                    // upload the stream with block id
                    var url = baseUrl + WebUtility.UrlEncode(byteArrayWithBlockId.BlockId);
                    try
                    {
                        await PutBlock(byteArrayWithBlockId.StreamAsByteArray,
                            client, url, cts, retryRequest: true);
                    }
                    catch (Exception)
                    {
                        //we failed to upload this block
                        //there is nothing to do here and we need to cancel all of the running tasks
                        cts.Cancel();
                        throw;
                    }
                }
            });
        }

        private class ByteArrayWithBlockId
        {
            public byte[] StreamAsByteArray { get; set; }
            public string BlockId { get; set; }
        }

        private async Task PutBlock(byte[] streamAsByteArray, HttpClient client,
            string url, CancellationTokenSource cts, bool retryRequest)
        {
            var now = SystemTime.UtcNow;
            //stream is disposed by the HttpClient
            var stream = new MemoryStream(streamAsByteArray);
            var content = new StreamContent(stream)
            {
                Headers =
                {
                    {"x-ms-date", now.ToString("R")},
                    {"x-ms-version", AzureStorageVersion},
                    {"Content-Length", stream.Length.ToString(CultureInfo.InvariantCulture)}
                }
            };

            client.DefaultRequestHeaders.Authorization = CalculateAuthorizationHeaderValue(HttpMethods.Put.Method, url, content.Headers);

            HttpResponseMessage response = null;
            try
            {
                response = await client.PutAsync(url, content, cts.Token);
                if (response.IsSuccessStatusCode)
                    return;
            }
            catch (Exception)
            {
                if (cts.IsCancellationRequested)
                    return;

                if (retryRequest == false)
                    throw;
            }

            if (retryRequest == false ||
                (response != null && response.StatusCode == HttpStatusCode.RequestEntityTooLarge))
                throw StorageException.FromResponseMessage(response);

            // wait for one second before trying again to send the request
            // maybe there was a network issue?
            await Task.Delay(1000);
            await PutBlock(streamAsByteArray, client, url, cts, retryRequest: false);
        }

        private async Task PutBlockList(string baseUrl, List<string> blockIds, Dictionary<string, string> metadata)
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

            var client = GetClient(TimeSpan.FromHours(1));
            client.DefaultRequestHeaders.Authorization = CalculateAuthorizationHeaderValue(HttpMethods.Put.Method, url, content.Headers);

            var response = await client.PutAsync(url, content);
            if (response.IsSuccessStatusCode)
                return;

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

        public async Task<Blob> GetBlob(string key)
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
            client.DefaultRequestHeaders.Authorization = CalculateAuthorizationHeaderValue(HttpMethods.Get.Method, url, requestMessage.Headers);

            var response = await client.SendAsync(requestMessage);
            if (response.StatusCode == HttpStatusCode.NotFound)
                return null;

            if (response.IsSuccessStatusCode == false)
                throw StorageException.FromResponseMessage(response);

            var data = await response.Content.ReadAsStreamAsync();
            var headers = response.Headers.ToDictionary(x => x.Key, x => x.Value.FirstOrDefault());

            return new Blob(data, headers);
        }

        public async Task TestConnection()
        {
            var url = _baseServerUrl + "?comp=list&maxresults=1";

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
            client.DefaultRequestHeaders.Authorization = CalculateAuthorizationHeaderValue(HttpMethods.Get.Method, url, requestMessage.Headers);

            var response = await client.SendAsync(requestMessage).ConfigureAwait(false);

            if (response.IsSuccessStatusCode == false)
                throw StorageException.FromResponseMessage(response);
        }

        private AuthenticationHeaderValue CalculateAuthorizationHeaderValue(string httpMethod, string url, HttpHeaders httpHeaders)
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

        private static string ComputeCanonicalizedHeaders(string httpMethod, HttpHeaders httpHeaders)
        {
            var headers = httpHeaders
                .Where(x => x.Key.StartsWith("x-ms-"))
                .OrderBy(x => x.Key);

            var httpMethodToUpper = httpMethod.ToUpper();

            var contentLength = httpMethodToUpper == "GET" ? string.Empty : "0";
            IEnumerable<string> values;
            if (httpHeaders.TryGetValues("Content-Length", out values))
                contentLength = values.First();

            var contentType = string.Empty;
            if (httpHeaders.TryGetValues("Content-Type", out values))
                contentType = values.First();

            var stringToHash = $"{httpMethodToUpper}\n\n\n{contentLength}\n\n{contentType}\n\n\n\n\n\n\n";

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