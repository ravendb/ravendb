using System;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Connection.Implementation;
using Raven.Client.Connection.Profiling;
using Raven.Client.Extensions;
using Raven.Client.FileSystem.Connection;
using Raven.Client.FileSystem.Extensions;
using Raven.Client.FileSystem.Listeners;
using Raven.Client.Util.Auth;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Client.FileSystem
{

    public class AsyncFilesServerClient : AsyncServerClientBase<FilesConvention, IFilesReplicationInformer>, IAsyncFilesCommandsImpl
    {
        private readonly IFilesConflictListener[] conflictListeners;
        private bool resolvingConflict = false;


        /// <summary>
        /// Notify when the failover status changed
        /// </summary>
        public event EventHandler<FailoverStatusChangedEventArgs> FailoverStatusChanged
        {
            add { ReplicationInformer.FailoverStatusChanged += value; }
            remove { ReplicationInformer.FailoverStatusChanged -= value; }
        }

        public AsyncFilesServerClient(string serverUrl, string fileSystemName, FilesConvention conventions, OperationCredentials credentials, HttpJsonRequestFactory requestFactory = null, Guid? sessionId = null, Func<string, IFilesReplicationInformer> replicationInformerGetter = null, IFilesConflictListener[] conflictListeners = null, NameValueCollection operationsHeaders = null)
            : base(serverUrl, conventions, credentials, requestFactory, sessionId, operationsHeaders, replicationInformerGetter, fileSystemName)
        {
            try
            {
                FileSystemName = fileSystemName;
                ApiKey = credentials.ApiKey;
                this.conflictListeners = conflictListeners ?? new IFilesConflictListener[0];
                if (replicationInformerGetter != null && ReplicationInformer != null)
                    ReplicationInformer.UpdateReplicationInformationIfNeeded(this);
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }
        }

        public AsyncFilesServerClient(string serverUrl, string fileSystemName, ICredentials credentials = null, string apiKey = null)
            : this(serverUrl, fileSystemName, null, new OperationCredentials(apiKey, credentials ?? CredentialCache.DefaultNetworkCredentials))
        {
        }

        protected override Func<string, IFilesReplicationInformer> DefaultReplicationInformerGetter()
        {
            return name => new FilesReplicationInformer(Conventions, RequestFactory);
        }

        protected override string BaseUrl
        {
            get { return UrlFor(); }
        }

        public override string UrlFor(string fileSystem = null)
        {
            if (string.IsNullOrWhiteSpace(fileSystem))
                fileSystem = FileSystemName;

            return ServerUrl + "/fs/" + Uri.EscapeDataString(fileSystem);
        }

        public string FileSystemName { get; private set; }

        public IAsyncFilesCommands ForFileSystem(string fileSystemName)
        {
            return new AsyncFilesServerClient(ServerUrl, fileSystemName, Conventions, PrimaryCredentials, RequestFactory, SessionId, ReplicationInformerGetter, conflictListeners, OperationsHeaders);
        }

        public IAsyncFilesCommands With(ICredentials credentials)
        {
            var primaryCredentials = new OperationCredentials(ApiKey, credentials);
            return new AsyncFilesServerClient(ServerUrl, FileSystemName, Conventions, primaryCredentials, RequestFactory, SessionId, ReplicationInformerGetter, conflictListeners, OperationsHeaders);
        }

        public IAsyncFilesCommands With(OperationCredentials credentials)
        {
            return new AsyncFilesServerClient(ServerUrl, FileSystemName, Conventions, credentials, RequestFactory, SessionId, ReplicationInformerGetter, conflictListeners, OperationsHeaders);
        }

        public string ApiKey { get; private set; }

        private async Task<RavenJToken> GetOperationStatusAsync(long id)
        {
            using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, BaseUrl + "/operation/status?id=" + id, HttpMethods.Get, CredentialsThatShouldBeUsedOnlyInOperationsWithoutReplication, Conventions).AddOperationHeaders(OperationsHeaders)))
            {
                try
                {
                    return await request.ReadResponseJsonAsync().ConfigureAwait(false);
                }
                catch (ErrorResponseException e)
                {
                    if (e.StatusCode == HttpStatusCode.NotFound) return null;
                    throw;
                }
            }
        }

        public Task<FileSystemStats> GetStatisticsAsync()
        {
            return ExecuteWithReplication(HttpMethods.Get, async (operation, requestTimeMetric) =>
            {
                var requestUriString = operation.Url + "/stats";
                using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethods.Get, operation.Credentials, Conventions)).AddOperationHeaders(OperationsHeaders))
                {
                    try
                    {
                        var response = (RavenJObject)await request.ReadResponseJsonAsync().ConfigureAwait(false);
                        return response.JsonDeserialization<FileSystemStats>();
                    }
                    catch (ErrorResponseException)
                    {
                        throw;
                    }
                    catch (Exception e)
                    {
                        throw e.SimplifyException();
                    }
                }
            });
        }

        public Task DeleteAsync(string filename, Etag etag = null)
        {
            return ExecuteWithReplication(HttpMethods.Delete, async (operation, requestTimeMetric) =>
            {
                var requestUriString = operation.Url + "/files/" + Uri.EscapeDataString(filename);

                using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethods.Delete, operation.Credentials, Conventions)).AddOperationHeaders(OperationsHeaders))
                {
                    AsyncFilesServerClientExtension.AddEtagHeader(request, etag);
                    try
                    {
                        await request.ExecuteRequestAsync().ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        throw e.SimplifyException();
                    }
                }
            });
        }

        public Task RenameAsync(string filename, string rename, Etag etag = null)
        {
            return ExecuteWithReplication(HttpMethods.Patch, async (operation, requestTimeMetric) =>
            {
                var requestUriString = operation.Url + "/files/" + Uri.EscapeDataString(filename) + "?rename=" + Uri.EscapeDataString(rename);

                using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethods.Patch, operation.Credentials, Conventions)).AddOperationHeaders(OperationsHeaders))
                {
                    AsyncFilesServerClientExtension.AddEtagHeader(request, etag);
                    try
                    {
                        await request.ExecuteRequestAsync().ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        throw e.SimplifyException();
                    }
                }
            });
        }

        public Task CopyAsync(string sourceName, string targetName, Etag etag = null)
        {
            return ExecuteWithReplication(HttpMethods.Post, async (operation, requestTimeMetric) =>
            {
                var requestUriString = operation.Url + "/files-copy/" + Uri.EscapeDataString(sourceName) + "?targetFilename=" + Uri.EscapeDataString(targetName);

                using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethods.Post, operation.Credentials, Conventions)).AddOperationHeaders(OperationsHeaders))
                {
                    AsyncFilesServerClientExtension.AddEtagHeader(request, etag);
                    try
                    {
                        await request.ExecuteRequestAsync().ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        throw e.SimplifyException();
                    }
                }
            });
        }

        public Task<FileHeader[]> BrowseAsync(int start = 0, int pageSize = 25)
        {
            return ExecuteWithReplication(HttpMethods.Get, async (operation, requestTimeMetric) =>
            {
                using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operation.Url + "/files?start=" + start + "&pageSize=" + pageSize, HttpMethods.Get, operation.Credentials, Conventions)).AddOperationHeaders(OperationsHeaders))
                {
                    try
                    {
                        var response = (RavenJArray)await request.ReadResponseJsonAsync().ConfigureAwait(false);
                        return response.JsonDeserialization<FileHeader>();
                    }
                    catch (Exception e)
                    {
                        throw e.SimplifyException();
                    }
                }
            });
        }


        public async Task<TouchFilesResult> TouchFilesAsync(Etag start, int pageSize)
        {
            var requestUrlString = string.Format("{0}/fs/{1}/files/touch?etag={2}&pageSize={3}", ServerUrl, Uri.EscapeDataString(FileSystemName), start, pageSize);

            using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUrlString, HttpMethods.Get, PrimaryCredentials, Conventions)))
            {
                try
                {
                    var response = await request.ReadResponseJsonAsync().ConfigureAwait(false);
                    return response.JsonDeserialization<TouchFilesResult>();
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            }
        }

        public Task<string[]> GetSearchFieldsAsync(int start = 0, int pageSize = 25)
        {
            return ExecuteWithReplication(HttpMethods.Get, async (operation, requestTimeMetric) =>
            {
                var requestUriString = string.Format("{0}/search/terms?start={1}&pageSize={2}", operation.Url, start, pageSize);
                using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethods.Get, operation.Credentials, Conventions).AddOperationHeaders(OperationsHeaders)))
                {
                    try
                    {
                        var response = (RavenJArray)await request.ReadResponseJsonAsync().ConfigureAwait(false);
                        return response.JsonDeserialization<string>();
                    }
                    catch (Exception e)
                    {
                        throw e.SimplifyException();
                    }
                }
            });
        }

        public Task<SearchResults> SearchAsync(string query, string[] sortFields = null, int start = 0, int pageSize = 25)
        {
            return ExecuteWithReplication(HttpMethods.Get, async (operation, requestTimeMetric) =>
            {
                var requestUriBuilder = new StringBuilder(operation.Url)
                    .Append("/search/?query=")
                    .Append(Uri.EscapeDataString(query))
                    .Append("&start=")
                    .Append(start)
                    .Append("&pageSize=")
                    .Append(pageSize);

                if (sortFields != null)
                {
                    foreach (var sortField in sortFields)
                    {
                        requestUriBuilder.Append("&sort=").Append(sortField);
                    }
                }

                using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriBuilder.ToString(), HttpMethods.Get, operation.Credentials, Conventions)).AddOperationHeaders(OperationsHeaders))
                {
                    try
                    {
                        var response = (RavenJObject)await request.ReadResponseJsonAsync().ConfigureAwait(false);
                        return response.JsonDeserialization<SearchResults>();
                    }
                    catch (Exception e)
                    {
                        throw e.SimplifyException();
                    }
                }
            });
        }

        public Task DeleteByQueryAsync(string query)
        {
            return ExecuteWithReplication(HttpMethods.Delete, async (operation, requestTimeMetric) =>
            {
                var requestUriString = string.Format("{0}/search?query={1}", operation.Url, Uri.EscapeDataString(query));

                using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethods.Delete, operation.Credentials, Conventions)).AddOperationHeaders(OperationsHeaders))
                {
                    try
                    {
                        var json = await request.ReadResponseJsonAsync().ConfigureAwait(false);
                        var operationId = json.Value<long>("OperationId");
                        var op = new Operation(GetOperationStatusAsync, operationId);
                        await op.WaitForCompletionAsync().ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        throw e.SimplifyException();
                    }
                }
            });
        }

        public Task<RavenJObject> GetMetadataForAsync(string filename)
        {
            return ExecuteWithReplication(HttpMethod.Head, (operation, requestTimeMetric) => AsyncFilesServerClientExtension.GetMetadataForAsyncImpl(this, RequestFactory, Conventions, OperationsHeaders, filename, operation.Url, operation.Credentials));
        }

        public Task<FileHeader[]> GetAsync(string[] filename)
        {
            return ExecuteWithReplication(HttpMethods.Get, (operation, requestTimeMetric) => GetAsyncImpl(filename, operation));
        }

        public Task<FileHeader[]> StartsWithAsync(string prefix, string matches, int start, int pageSize)
        {
            return ExecuteWithReplication(HttpMethods.Get, (operation, requestTimeMetric) => StartsWithAsyncImpl(prefix, matches, start, pageSize, operation));
        }

        public async Task<IAsyncEnumerator<FileHeader>> StreamFileHeadersAsync(Etag fromEtag, int pageSize = int.MaxValue)
        {
            if (fromEtag == null)
                throw new ArgumentException("fromEtag");

            var operationMetadata = new OperationMetadata(this.BaseUrl, this.CredentialsThatShouldBeUsedOnlyInOperationsWithoutReplication, null);

            var sb = new StringBuilder(operationMetadata.Url)
                .Append("/streams/files?etag=")
                .Append(fromEtag)
                .Append("&pageSize=")
                .Append(pageSize);

            var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, sb.ToString(), HttpMethods.Get, operationMetadata.Credentials, this.Conventions, timeout: TimeSpan.FromMinutes(15))
                                        .AddOperationHeaders(OperationsHeaders));

            request.RemoveAuthorizationHeader();

            var tokenRetriever = new SingleAuthTokenRetriever(this, RequestFactory, Conventions, OperationsHeaders, operationMetadata);

            var token = await tokenRetriever.GetToken().ConfigureAwait(false);
            try
            {
                token = await tokenRetriever.ValidateThatWeCanUseToken(token).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                request.Dispose();

                throw new InvalidOperationException(
                    "Could not authenticate token for query streaming, if you are using ravendb in IIS make sure you have Anonymous Authentication enabled in the IIS configuration",
                    e);
            }

            request.AddOperationHeader("Single-Use-Auth-Token", token);

            HttpResponseMessage response;

            try
            {
                response = await request.ExecuteRawResponseAsync()
                    .ConfigureAwait(false);

                await response.AssertNotFailingResponse().ConfigureAwait(false);
            }
            catch (Exception)
            {
                request.Dispose();

                throw;
            }

            return new YieldStreamResults(request, await response.GetResponseStreamWithHttpDecompression().ConfigureAwait(false),
                () =>
                {
                    response.Content?.Dispose();
                    response.Dispose();
                });
        }

        private async Task<IAsyncEnumerator<FileHeader>> StreamQueryAsyncImpl(string query, string[] sortFields, int start, int pageSize, OperationMetadata operationMetadata)
        {
            var path = new StringBuilder(operationMetadata.Url)
                .Append("/streams/query?query=")
                .Append(Uri.EscapeDataString(query))
                .Append("&start=")
                .Append(start)
                .Append("&pageSize=")
                .Append(pageSize);

            if (sortFields != null)
            {
                foreach (var sortField in sortFields)
                {
                    path.Append("&sort=").Append(sortField);
                }
            }

            var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, path.ToString().Trim(), HttpMethods.Get, operationMetadata.Credentials, Conventions, timeout: TimeSpan.FromMinutes(15))
                                            .AddOperationHeaders(OperationsHeaders));

            request.RemoveAuthorizationHeader();

            var tokenRetriever = new SingleAuthTokenRetriever(this, RequestFactory, Conventions, OperationsHeaders, operationMetadata);

            var token = await tokenRetriever.GetToken().ConfigureAwait(false);

            try
            {
                token = await tokenRetriever.ValidateThatWeCanUseToken(token).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                request.Dispose();

                throw new InvalidOperationException(
                    "Could not authenticate token for query streaming, if you are using ravendb in IIS make sure you have Anonymous Authentication enabled in the IIS configuration",
                    e);
            }

            request.AddOperationHeader("Single-Use-Auth-Token", token);

            HttpResponseMessage response;

            try
            {
                response = await request.ExecuteRawResponseAsync().ConfigureAwait(false);

                await response.AssertNotFailingResponse().ConfigureAwait(false);
            }
            catch (Exception)
            {
                request.Dispose();

                throw;
            }

            return new YieldStreamResults(request, await response.GetResponseStreamWithHttpDecompression().ConfigureAwait(false),
                () =>
                {
                    response.Content?.Dispose();
                    response.Dispose();
                });
        }

        public Task<IAsyncEnumerator<FileHeader>> StreamQueryAsync(string query, string[] sortFields = null, int start = 0, int pageSize = int.MaxValue)
        {
            return ExecuteWithReplication(HttpMethod.Get, (operation, requestTimeMetric) => StreamQueryAsyncImpl(query, sortFields, start, pageSize, operation));
        }

        internal class YieldStreamResults : IAsyncEnumerator<FileHeader>
        {
            private readonly HttpJsonRequest request;

            private readonly Stream stream;
            private readonly Action onDispose;
            private readonly StreamReader streamReader;
            private readonly JsonTextReaderAsync reader;
            private bool complete;

            private bool wasInitialized;

            public YieldStreamResults(HttpJsonRequest request, Stream stream, Action onDispose = null)
            {
                this.request = request;
                this.stream = stream;
                this.onDispose = onDispose;
                streamReader = new StreamReader(stream);
                reader = new JsonTextReaderAsync(streamReader);
            }

            private async Task InitAsync()
            {
                if (await reader.ReadAsync().ConfigureAwait(false) == false || reader.TokenType != JsonToken.StartObject)
                    throw new InvalidOperationException("Unexpected data at start of stream");

                if (await reader.ReadAsync().ConfigureAwait(false) == false || reader.TokenType != JsonToken.PropertyName || Equals("Results", reader.Value) == false)
                    throw new InvalidOperationException("Unexpected data at stream 'Results' property name");

                if (await reader.ReadAsync().ConfigureAwait(false) == false || reader.TokenType != JsonToken.StartArray)
                    throw new InvalidOperationException("Unexpected data at 'Results', could not find start results array");
            }

            public void Dispose()
            {
                reader.Close();
#if !DNXCORE50
                streamReader.Close();
                stream.Close();
#else
                streamReader.Dispose();
                stream.Dispose();
#endif
                request.Dispose();

                onDispose?.Invoke();
            }

            public async Task<bool> MoveNextAsync()
            {
                if (complete)
                {
                    // to parallel IEnumerable<T>, subsequent calls to MoveNextAsync after it has returned false should
                    // also return false, rather than throwing
                    return false;
                }

                if (wasInitialized == false)
                {
                    await InitAsync().ConfigureAwait(false);
                    wasInitialized = true;
                }

                if (await reader.ReadAsync().ConfigureAwait(false) == false)
                    throw new InvalidOperationException("Unexpected end of data");

                if (reader.TokenType == JsonToken.EndArray)
                {
                    complete = true;

                    await TryReadNextPageStart().ConfigureAwait(false);

                    await EnsureValidEndOfResponse().ConfigureAwait(false);
                    this.Dispose();
                    return false;
                }

                var receivedObject = (RavenJObject)await RavenJToken.ReadFromAsync(reader).ConfigureAwait(false);
                Current = receivedObject.JsonDeserialization<FileHeader>();
                return true;
            }

            private async Task TryReadNextPageStart()
            {
                if (!(await reader.ReadAsync().ConfigureAwait(false)) || reader.TokenType != JsonToken.PropertyName)
                    return;

                switch ((string)reader.Value)
                {
                    case "Error":
                        var err = await reader.ReadAsString().ConfigureAwait(false);
                        throw new InvalidOperationException("Server error" + Environment.NewLine + err);
                    default:
                        throw new InvalidOperationException("Unexpected property name: " + reader.Value);
                }

            }

            private async Task EnsureValidEndOfResponse()
            {
                if (reader.TokenType != JsonToken.EndObject && await reader.ReadAsync().ConfigureAwait(false) == false)
                    throw new InvalidOperationException("Unexpected end of response - missing EndObject token");

                if (reader.TokenType != JsonToken.EndObject)
                    throw new InvalidOperationException(string.Format("Unexpected token type at the end of the response: {0}. Error: {1}", reader.TokenType, streamReader.ReadToEnd()));

                var remainingContent = streamReader.ReadToEnd();

                if (string.IsNullOrEmpty(remainingContent) == false)
                    throw new InvalidOperationException("Server error: " + remainingContent);
            }

            public FileHeader Current { get; private set; }
        }

        private async Task<FileHeader[]> StartsWithAsyncImpl(string prefix, string matches, int start, int pageSize, OperationMetadata operation)
        {
            var uri = string.Format("/files?startsWith={0}&matches={1}&start={2}&pageSize={3}", string.IsNullOrEmpty(prefix) ? null : Uri.EscapeDataString(prefix), string.IsNullOrEmpty(matches) ? null : Uri.EscapeDataString(matches), start, pageSize);

            using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operation.Url + uri, HttpMethods.Get, operation.Credentials, Conventions)).AddOperationHeaders(OperationsHeaders))
            {
                try
                {
                    var response = (RavenJArray)await request.ReadResponseJsonAsync().ConfigureAwait(false);
                    return response.JsonDeserialization<FileHeader>();
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            }
        }

        private async Task<FileHeader[]> GetAsyncImpl(string[] filenames, OperationMetadata operation)
        {
            var requestUriBuilder = new StringBuilder("/files?");
            for (int i = 0; i < filenames.Length; i++)
            {
                requestUriBuilder.Append("fileNames=" + Uri.EscapeDataString(filenames[i]));
                if (i < filenames.Length - 1)
                    requestUriBuilder.Append("&");
            }

            using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operation.Url + requestUriBuilder.ToString(), HttpMethods.Get, operation.Credentials, Conventions)).AddOperationHeaders(OperationsHeaders))
            {
                try
                {
                    var response = (RavenJArray)await request.ReadResponseJsonAsync().ConfigureAwait(false);

                    var results = response.JsonDeserialization<FileHeader>();

                    results.ForEach(x =>
                    {
                        if (x == null)
                            return;

                        x.Metadata = new RavenJObject(x.Metadata, StringComparer.OrdinalIgnoreCase); // ensure metadata keys aren't case sensitive
                    });

                    return results;
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            }
        }

        public Task<Stream> DownloadAsync(string filename, Reference<RavenJObject> metadataRef = null, long? from = null, long? to = null)
        {
            return ExecuteWithReplication(HttpMethod.Get, async (operation, requestTimeMetric) => await AsyncFilesServerClientExtension.DownloadAsyncImpl(this, RequestFactory, Conventions, OperationsHeaders, "/files/", filename, metadataRef, from, to, operation.Url, operation.Credentials).ConfigureAwait(false));
        }

        public Task UpdateMetadataAsync(string filename, RavenJObject metadata, Etag etag = null)
        {
            return ExecuteWithReplication(HttpMethods.Post, async (operation, requestTimeMetric) =>
            {
                using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operation.Url + "/files/" + filename, HttpMethods.Post, operation.Credentials, Conventions)).AddOperationHeaders(OperationsHeaders))
                {
                    AddHeaders(metadata, request);
                    AsyncFilesServerClientExtension.AddEtagHeader(request, etag);
                    try
                    {
                        await request.ExecuteRequestAsync().ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        throw e.SimplifyException();
                    }
                }
            });
        }

        public Task UploadAsync(string filename, Stream source, RavenJObject metadata = null, Etag etag = null)
        {
            if (source.CanRead == false)
                throw new Exception("Stream does not support reading");

            var streamConsumed = false;
            long start = -1;

            long? size = null;
            if (source.CanSeek)
            {
                size = source.Length;
                start = source.Position;
            }

            if (metadata == null)
                metadata = new RavenJObject();

            return ExecuteWithReplication(HttpMethods.Put, async (operation, requestTimeMetric) =>
            {
                await UploadAsyncImpl(operation, filename, source.CopyToAsync, () =>
                {
                    if (streamConsumed)
                    {
                        // If the content needs to be written to a target stream a 2nd time, then the stream must support
                        // seeking, otherwise the stream can't be copied a second time to a target stream (e.g. a NetworkStream).
                        if (source.CanSeek)
                            source.Position = start;
                        else
                            throw new InvalidOperationException("We need to resend the request body while the stream was already consumed. It cannot be read again because it's not seekable");
                    }

                    streamConsumed = true;
                }, metadata, false, size, etag).ConfigureAwait(false);
            });
        }

        public Task UploadAsync(string filename, Action<Stream> source, Action prepareStream, long? size, RavenJObject metadata = null, Etag etag = null)
        {
            if (metadata == null)
                metadata = new RavenJObject();

            return ExecuteWithReplication(HttpMethods.Put, async (operation, requestTimeMetric) =>
            {
                await UploadAsyncImpl(operation, filename, stream =>
                {
                    source(stream);
                    return new CompletedTask();
                }, prepareStream, metadata, false, size, etag).ConfigureAwait(false);
            });
        }

        public Task UploadRawAsync(string filename, Stream source, RavenJObject metadata, long? size, Etag etag = null)
        {
            var operationMetadata = new OperationMetadata(BaseUrl, CredentialsThatShouldBeUsedOnlyInOperationsWithoutReplication, null);
            return UploadAsyncImpl(operationMetadata, filename, source.CopyToAsync, () => { }, metadata, true, size, etag);
        }

        private async Task UploadAsyncImpl(OperationMetadata operation, string filename, Func<Stream, Task> source, Action prepareStream, RavenJObject metadata, bool preserveTimestamps, long? size, Etag etag)
        {
            var operationUrl = operation.Url + "/files?name=" + Uri.EscapeDataString(filename);
            if (preserveTimestamps)
                operationUrl += "&preserveTimestamps=true";

            var createHttpJsonRequestParams = new CreateHttpJsonRequestParams(this, operationUrl, HttpMethod.Put, operation.Credentials, Conventions, timeout: TimeSpan.FromHours(12))
            {
                DisableRequestCompression = true
            };

            using (var request = RequestFactory.CreateHttpJsonRequest(createHttpJsonRequestParams).AddOperationHeaders(OperationsHeaders))
            using (ConnectionOptions.Expect100Continue(request.Url))
            {
                if (size.HasValue)
                    metadata[Constants.FileSystem.RavenFsSize] = new RavenJValue(size);

                AddHeaders(metadata, request);
                AsyncFilesServerClientExtension.AddEtagHeader(request, etag);

                var response = await request.ExecuteRawRequestAsync(async(netStream, t) =>
                {
                    try
                    {
                        prepareStream?.Invoke();

                        await source(netStream).ConfigureAwait(false);
                        netStream.Flush();

                        t.TrySetResult(null);
                    }
                    catch (Exception e)
                    {
                        t.TrySetException(e);
                    }
                }).ConfigureAwait(false);


                try
                {
                    await response.AssertNotFailingResponse().ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    var simplified = e.SimplifyException();

                    if (simplified != e)
                        throw simplified;

                    throw;
                }
            }
        }

        internal async Task<bool> TryResolveConflictByUsingRegisteredListenersAsync(string filename, FileHeader remote, string sourceServerUri, Action beforeConflictResolution)
        {
            var files = await this.GetAsync(new[] { filename }).ConfigureAwait(false);
            FileHeader local = files.FirstOrDefault();

            // File does not exists anymore on the server.
            if (local == null)
                return false;

            if (conflictListeners.Length > 0 && resolvingConflict == false)
            {
                resolvingConflict = true;
                try
                {
                    var resolutionStrategy = ConflictResolutionStrategy.NoResolution;
                    foreach (var conflictListener in conflictListeners)
                    {
                        var strategy = conflictListener.ConflictDetected(local, remote, sourceServerUri);
                        if (strategy != ConflictResolutionStrategy.NoResolution)
                        {
                            resolutionStrategy = strategy;
                            break;
                        }
                    }

                    // We execute an external action before conflict resolution starts.
                    beforeConflictResolution();

                    if (resolutionStrategy == ConflictResolutionStrategy.NoResolution)
                        return false;

                    // We resolve the conflict.
                    try
                    {
                        var client = new SynchronizationClient(this, Conventions);
                        await client.ResolveConflictAsync(filename, resolutionStrategy).ConfigureAwait(false);

                        // Refreshing the file information.
                        files = await this.GetAsync(new[] { filename }).ConfigureAwait(false);
                        files.ApplyIfNotNull(x =>
                      {
                          // We notify the listeners.
                          foreach (var conflictListener in conflictListeners)
                              conflictListener.ConflictResolved(x);
                      });

                        return true;
                    }
                    catch { }
                }
                finally
                {
                    resolvingConflict = false;
                }
            }
            else // No resolution listeners, therefore we notify the subscribers.
            {
                beforeConflictResolution();
            }

            return false;
        }

        public IAsyncFilesSynchronizationCommands Synchronization
        {
            get { return new SynchronizationClient(this, Conventions); }
        }

        public IAsyncFilesConfigurationCommands Configuration
        {
            get { return new ConfigurationClient(this, Conventions); }
        }

        public IAsyncFilesStorageCommands Storage
        {
            get { return new StorageClient(this, Conventions); }
        }

        public IAsyncFilesAdminCommands Admin
        {
            get { return new AdminClient(this, Conventions); }
        }

        private static void AddHeaders(RavenJObject metadata, HttpJsonRequest request)
        {
            foreach (var item in metadata)
            {
                var value = item.Value is RavenJValue ? item.Value.ToString() : item.Value.ToString(Formatting.None);
                request.AddHeader(item.Key, value);
            }
        }

        public Task<string[]> GetDirectoriesAsync(string from = null, int start = 0, int pageSize = 25)
        {
            return ExecuteWithReplication(HttpMethods.Get, async (operation, requestTimeMetric) =>
            {
                var path = @from ?? "";
                if (path.StartsWith("/"))
                    path = path.Substring(1);

                var requestUriString = operation.Url + "/folders/subdirectories/" + Uri.EscapeDataString(path) + "?pageSize=" +
                                       pageSize + "&start=" + start;

                using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethods.Get, operation.Credentials, Conventions)).AddOperationHeaders(OperationsHeaders))
                {
                    try
                    {
                        var response = (RavenJArray)await request.ReadResponseJsonAsync().ConfigureAwait(false);
                        return response.JsonDeserialization<string>();
                    }
                    catch (Exception e)
                    {
                        throw e.SimplifyException();
                    }
                }
            });
        }

        public Task<SearchResults> SearchOnDirectoryAsync(string folder, FilesSortOptions options = FilesSortOptions.Default,
                                                     string fileNameSearchPattern = "", int start = 0, int pageSize = 25)
        {
            var folderQueryPart = GetFolderQueryPart(folder);

            if (string.IsNullOrEmpty(fileNameSearchPattern) == false && fileNameSearchPattern.Contains("*") == false && fileNameSearchPattern.Contains("?") == false)
            {
                fileNameSearchPattern = fileNameSearchPattern + "*";
            }
            var fileNameQueryPart = GetFileNameQueryPart(fileNameSearchPattern);

            return SearchAsync(folderQueryPart + fileNameQueryPart, GetSortFields(options), start, pageSize);
        }

        public Task<Guid> GetServerIdAsync()
        {
            return ExecuteWithReplication(HttpMethods.Get, async (operation, requestTimeMetric) =>
            {
                var requestUriString = operation.Url + "/static/id";

                using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethods.Get, operation.Credentials, Conventions)).AddOperationHeaders(OperationsHeaders))
                {
                    try
                    {
                        var response = (RavenJValue)await request.ReadResponseJsonAsync().ConfigureAwait(false);
                        return response.Value<Guid>();
                    }
                    catch (Exception e)
                    {
                        throw e.SimplifyException();
                    }
                }
            });
        }

        private static string GetFileNameQueryPart(string fileNameSearchPattern)
        {
            if (string.IsNullOrEmpty(fileNameSearchPattern))
                return "";

            if (fileNameSearchPattern.StartsWith("*") || (fileNameSearchPattern.StartsWith("?")))
                return " AND __rfileName:" + Reverse(fileNameSearchPattern);

            return " AND __fileName:" + fileNameSearchPattern;
        }

        private static string Reverse(string value)
        {
            var characters = value.ToCharArray();
            Array.Reverse(characters);

            return new string(characters);
        }

        private static string GetFolderQueryPart(string folder)
        {
            if (folder == null) throw new ArgumentNullException("folder");
            if (folder.StartsWith("/") == false)
                throw new ArgumentException("folder must starts with a /", "folder");

            int level;
            if (folder == "/")
                level = 1;
            else
                level = folder.Count(ch => ch == '/') + 1;

            var folderQueryPart = "__directoryName:" + folder + " AND __level:" + level;
            return folderQueryPart;
        }

        private static string[] GetSortFields(FilesSortOptions options)
        {
            string sort = null;
            switch (options & ~FilesSortOptions.Desc)
            {
                case FilesSortOptions.Name:
                    sort = "__key";
                    break;
                case FilesSortOptions.Size:
                    sort = "__size";
                    break;
                case FilesSortOptions.LastModified:
                    sort = "__modified";
                    break;
            }

            if (options.HasFlag(FilesSortOptions.Desc))
            {
                if (string.IsNullOrEmpty(sort))
                    throw new ArgumentException("databaseOptions");
                sort = "-" + sort;
            }

            var sortFields = string.IsNullOrEmpty(sort) ? null : new[] { sort };
            return sortFields;
        }

        private class ConfigurationClient : IAsyncFilesConfigurationCommands, IHoldProfilingInformation
        {
            private readonly AsyncFilesServerClient client;
            private readonly FilesConvention convention;

            public IAsyncFilesCommands Commands
            {
                get { return this.client; }
            }

            public ConfigurationClient(AsyncFilesServerClient client, FilesConvention convention)
            {
                this.client = client;
                this.convention = convention;
            }

            public Task<string[]> GetKeyNamesAsync(int start = 0, int pageSize = 25)
            {
                return client.ExecuteWithReplication(HttpMethods.Get, async (operation, requestTimeMetric) =>
                {
                    var requestUriString = operation.Url + "/config?start=" + start + "&pageSize=" + pageSize;

                    using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethods.Get, operation.Credentials, convention)).AddOperationHeaders(client.OperationsHeaders))
                    {
                        try
                        {
                            var response = (RavenJArray)await request.ReadResponseJsonAsync().ConfigureAwait(false);
                            return response.JsonDeserialization<string>();
                        }
                        catch (Exception e)
                        {
                            throw e.SimplifyException();
                        }
                    }
                });
            }

            public Task SetKeyAsync<T>(string name, T data)
            {
                return client.ExecuteWithReplication(HttpMethods.Put, async (operation, requestTimeMetric) =>
                {
                    var requestUriString = operation.Url + "/config?name=" + Uri.EscapeDataString(name);
                    using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethods.Put, operation.Credentials, convention)).AddOperationHeaders(client.OperationsHeaders))
                    {
                        var jsonData = data as RavenJObject;
                        if (jsonData != null)
                        {
                            await request.WriteAsync(jsonData).ConfigureAwait(false);
                        }
                        else if (data is NameValueCollection)
                        {
                            throw new ArgumentException("NameValueCollection objects are not supported to be stored in RavenFS configuration");
                        }
                        else
                        {
                            await request.WriteWithObjectAsync(data).ConfigureAwait(false);
                        }
                    }
                });
            }

            public Task DeleteKeyAsync(string name)
            {
                return client.ExecuteWithReplication(HttpMethods.Delete, async (operation, requestTimeMetric) =>
                {
                    var requestUriString = operation.Url + "/config?name=" + Uri.EscapeDataString(name);

                    using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethods.Delete, operation.Credentials, convention)).AddOperationHeaders(client.OperationsHeaders))
                    {
                        await request.ExecuteRequestAsync().ConfigureAwait(false);
                    }
                });
            }

            public Task<T> GetKeyAsync<T>(string name)
            {
                return client.ExecuteWithReplication(HttpMethods.Get, async (operation, requestTimeMetric) =>
                {
                    var requestUriString = operation.Url + "/config?name=" + Uri.EscapeDataString(name);

                    using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethods.Get, operation.Credentials, convention)).AddOperationHeaders(client.OperationsHeaders))
                    {
                        try
                        {
                            var response = (RavenJObject)await request.ReadResponseJsonAsync().ConfigureAwait(false);
                            return response.JsonDeserialization<T>();
                        }
                        catch (Exception e)
                        {
                            var responseException = e as ErrorResponseException;
                            if (responseException == null)
                                throw;

                            if (responseException.StatusCode == HttpStatusCode.NotFound)
                                return default(T);

                            throw;
                        }
                    }
                });
            }

            public Task<ConfigurationSearchResults> SearchAsync(string prefix, int start = 0, int pageSize = 25)
            {
                return client.ExecuteWithReplication(HttpMethods.Get, async (operation, requestTimeMetric) =>
                {
                    var requestUriBuilder = new StringBuilder(operation.Url)
                        .Append("/config/search/?prefix=")
                        .Append(Uri.EscapeDataString(prefix))
                        .Append("&start=")
                        .Append(start)
                        .Append("&pageSize=")
                        .Append(pageSize);

                    using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriBuilder.ToString(), HttpMethods.Get, operation.Credentials, convention)).AddOperationHeaders(client.OperationsHeaders))
                    {
                        try
                        {
                            var response = (RavenJObject)await request.ReadResponseJsonAsync().ConfigureAwait(false);
                            return response.JsonDeserialization<ConfigurationSearchResults>();
                        }
                        catch (Exception e)
                        {
                            throw e.SimplifyException();
                        }
                    }
                });
            }

            public void Dispose()
            {
            }

            public ProfilingInformation ProfilingInformation { get; private set; }
        }

        private class SynchronizationClient : SynchronizationServerClient, IAsyncFilesSynchronizationCommands
        {
            private readonly OperationCredentials credentials;
            private readonly FilesConvention convention;
            private readonly AsyncFilesServerClient client;

            public IAsyncFilesCommands Commands
            {
                get { return client; }
            }

            public SynchronizationClient(AsyncFilesServerClient client, FilesConvention convention)
                : base(client.ServerUrl, client.FileSystemName, client.ApiKey, client.PrimaryCredentials.Credentials, client.RequestFactory,
                    client.Conventions, client.PrimaryCredentials, client.OperationsHeaders)
            {
                credentials = client.PrimaryCredentials;
                this.convention = convention;
                this.client = client;
            }

            public async Task<SynchronizationDestination[]> GetDestinationsAsync()
            {
                var requestUriString = BaseUrl + "/config?name=" + Uri.EscapeDataString(SynchronizationConstants.RavenSynchronizationDestinations);

                using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethods.Get, Credentials, convention)).AddOperationHeaders(OperationsHeaders))
                {
                    try
                    {
                        var response = (RavenJObject)await request.ReadResponseJsonAsync().ConfigureAwait(false);
                        var rawDestinations = (RavenJArray)response["Destinations"];
                        return rawDestinations.JsonDeserialization<SynchronizationDestination>();
                    }
                    catch (Exception e)
                    {
                        var responseException = e as ErrorResponseException;
                        if (responseException == null)
                            throw e.SimplifyException();

                        if (responseException.StatusCode == HttpStatusCode.NotFound)
                            return null;

                        throw responseException;
                    }
                }
            }

            public Task SetDestinationsAsync(params SynchronizationDestination[] destinations)
            {
                var requestUriString = BaseUrl + "/config?name=" + Uri.EscapeDataString(SynchronizationConstants.RavenSynchronizationDestinations);
                using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethods.Put, Credentials, convention)).AddOperationHeaders(OperationsHeaders))
                {
                    var data = new { Destinations = destinations };

                    try
                    {
                        return request.WriteWithObjectAsync(data);
                    }
                    catch (Exception e)
                    {
                        throw e.SimplifyException();
                    }
                }
            }

            public async Task<ItemsPage<ConflictItem>> GetConflictsAsync(int start = 0, int pageSize = 25)
            {
                var requestUriString = string.Format("{0}/synchronization/conflicts?start={1}&pageSize={2}", BaseUrl, start,
                                                         pageSize);

                using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethod.Get, credentials, convention)).AddOperationHeaders(OperationsHeaders))
                {
                    try
                    {
                        var response = (RavenJObject)await request.ReadResponseJsonAsync().ConfigureAwait(false);
                        return response.JsonDeserialization<ItemsPage<ConflictItem>>();
                    }
                    catch (Exception e)
                    {
                        throw e.SimplifyException();
                    }
                }
            }

            public async Task<SynchronizationReport> GetSynchronizationStatusForAsync(string fileName)
            {
                var requestUriString = string.Format("{0}/synchronization/status?fileName={1}", BaseUrl, Uri.EscapeDataString(fileName));

                using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethod.Get, credentials, convention)).AddOperationHeaders(OperationsHeaders))
                {
                    try
                    {
                        var response = (RavenJObject)await request.ReadResponseJsonAsync().ConfigureAwait(false);
                        return response.JsonDeserialization<SynchronizationReport>();
                    }
                    catch (Exception e)
                    {
                        throw e.SimplifyException();
                    }
                }
            }

            public async Task<ItemsPage<SynchronizationReport>> GetFinishedAsync(int start = 0, int pageSize = 25)
            {
                var requestUriString = string.Format("{0}/synchronization/finished?start={1}&pageSize={2}", BaseUrl, start, pageSize);

                using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethod.Get, credentials, convention)).AddOperationHeaders(OperationsHeaders))
                {
                    try
                    {
                        var response = (RavenJObject)await request.ReadResponseJsonAsync().ConfigureAwait(false);
                        return response.JsonDeserialization<ItemsPage<SynchronizationReport>>();
                    }
                    catch (Exception e)
                    {
                        throw e.SimplifyException();
                    }
                }
            }

            public async Task<ItemsPage<SynchronizationDetails>> GetActiveAsync(int start = 0, int pageSize = 25)
            {
                var requestUriString = string.Format("{0}/synchronization/active?start={1}&pageSize={2}", BaseUrl, start, pageSize);

                using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethod.Get, credentials, convention)).AddOperationHeaders(OperationsHeaders))
                {
                    try
                    {
                        var response = (RavenJObject)await request.ReadResponseJsonAsync().ConfigureAwait(false);
                        return response.JsonDeserialization<ItemsPage<SynchronizationDetails>>();
                    }
                    catch (Exception e)
                    {
                        throw e.SimplifyException();
                    }
                }
            }

            public async Task<ItemsPage<SynchronizationDetails>> GetPendingAsync(int start = 0, int pageSize = 25)
            {
                var requestUriString = string.Format("{0}/synchronization/pending?start={1}&pageSize={2}", BaseUrl, start, pageSize);

                using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethod.Get, credentials, convention)).AddOperationHeaders(OperationsHeaders))
                {
                    try
                    {
                        var response = (RavenJObject)await request.ReadResponseJsonAsync().ConfigureAwait(false);
                        return response.JsonDeserialization<ItemsPage<SynchronizationDetails>>();
                    }
                    catch (Exception e)
                    {
                        throw e.SimplifyException();
                    }
                }
            }

            public async Task<DestinationSyncResult[]> StartAsync(bool forceSyncingAll = false)
            {
                var requestUriString = string.Format("{0}/synchronization/ToDestinations?forceSyncingAll={1}", BaseUrl, forceSyncingAll);

                using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethod.Post, credentials, convention)).AddOperationHeaders(OperationsHeaders))
                {
                    try
                    {
                        var response = (RavenJArray)await request.ReadResponseJsonAsync().ConfigureAwait(false);
                        return response.JsonDeserialization<DestinationSyncResult>();
                    }
                    catch (Exception e)
                    {
                        throw e.SimplifyException();
                    }
                }
            }

            public Task<SynchronizationReport> StartAsync(string fileName, IAsyncFilesCommands destination)
            {
                return StartAsync(fileName, destination.ToSynchronizationDestination());
            }

            public async Task<SynchronizationReport> StartAsync(string fileName, SynchronizationDestination destination)
            {
                var requestUriString = string.Format("{0}/synchronization/start/{1}", BaseUrl, Uri.EscapeDataString(fileName));

                using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethod.Post, credentials, convention)).AddOperationHeaders(OperationsHeaders))
                {
                    try
                    {
                        await request.WriteWithObjectAsync(destination).ConfigureAwait(false);

                        var response = (RavenJObject)await request.ReadResponseJsonAsync().ConfigureAwait(false);
                        return response.JsonDeserialization<SynchronizationReport>();
                    }
                    catch (Exception e)
                    {
                        throw e.SimplifyException();
                    }
                }
            }

            public void Dispose()
            {
            }
        }

        private class StorageClient : IAsyncFilesStorageCommands, IHoldProfilingInformation
        {
            private readonly AsyncFilesServerClient client;
            private readonly FilesConvention convention;

            public IAsyncFilesCommands Commands
            {
                get { return this.client; }
            }

            public StorageClient(AsyncFilesServerClient ravenFileSystemClient, FilesConvention convention)
            {
                this.client = ravenFileSystemClient;
                this.convention = convention;
            }

            public Task CleanUpAsync()
            {
                return client.ExecuteWithReplication(HttpMethods.Post, async (operation, requestTimeMetric) =>
                {
                    var requestUriString = string.Format("{0}/storage/cleanup", operation.Url);

                    using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethods.Post, operation.Credentials, convention)).AddOperationHeaders(client.OperationsHeaders))
                    {
                        try
                        {
                            await request.ExecuteRequestAsync().ConfigureAwait(false);
                        }
                        catch (Exception e)
                        {
                            throw e.SimplifyException();
                        }
                    }
                });
            }

            public Task RetryRenamingAsync()
            {
                return client.ExecuteWithReplication(HttpMethods.Post, async (operation, requestTimeMetric) =>
                {
                    var requestUriString = string.Format("{0}/storage/retryrenaming", operation.Url);

                    using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethods.Post, operation.Credentials, convention)).AddOperationHeaders(client.OperationsHeaders))
                    {
                        try
                        {
                            await request.ExecuteRequestAsync().ConfigureAwait(false);
                        }
                        catch (Exception e)
                        {
                            throw e.SimplifyException();
                        }
                    }
                });
            }

            public Task RetryCopyingAsync()
            {
                return client.ExecuteWithReplication(HttpMethod.Post, async (operation, requestTimeMetric) =>
                {
                    var requestUriString = String.Format("{0}/storage/retrycopying", operation.Url);

                    using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethod.Post, operation.Credentials, convention)).AddOperationHeaders(client.OperationsHeaders))
                    {
                        try
                        {
                            await request.ExecuteRequestAsync().ConfigureAwait(false);
                        }
                        catch (Exception e)
                        {
                            throw e.SimplifyException();
                        }
                    }
                });
            }

            public ProfilingInformation ProfilingInformation { get; private set; }

            public void Dispose()
            {
            }
        }

        private class AdminClient : IAsyncFilesAdminCommands, IHoldProfilingInformation
        {
            private readonly AsyncFilesServerClient client;
            private readonly FilesConvention convention;

            public IAsyncFilesCommands Commands
            {
                get { return this.client; }
            }


            public AdminClient(AsyncFilesServerClient client, FilesConvention convention)
            {
                this.client = client;
                this.convention = convention;
            }

            public async Task<string[]> GetNamesAsync()
            {
                var requestUriString = string.Format("{0}/fs", client.ServerUrl);

                using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethods.Get, client.PrimaryCredentials, convention)))
                {
                    try
                    {
                        var response = (RavenJArray)await request.ReadResponseJsonAsync().ConfigureAwait(false);
                        return response.JsonDeserialization<string>();
                    }
                    catch (Exception e)
                    {
                        throw e.SimplifyException();
                    }
                }
            }

            public async Task<FileSystemStats[]> GetStatisticsAsync()
            {
                var requestUriString = string.Format("{0}/fs/stats", client.ServerUrl);

                using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethods.Get, client.PrimaryCredentials, convention)))
                {
                    try
                    {
                        var response = (RavenJArray)await request.ReadResponseJsonAsync().ConfigureAwait(false);
                        return response.JsonDeserialization<FileSystemStats>();
                    }
                    catch (Exception e)
                    {
                        throw e.SimplifyException();
                    }
                }
            }

            public async Task CreateFileSystemAsync(FileSystemDocument filesystemDocument)
            {
                string newFileSystemName = filesystemDocument.Id.Replace(Constants.FileSystem.Prefix, "");
                var requestUriString = string.Format("{0}/admin/fs/{1}", client.ServerUrl,
                                                     newFileSystemName ?? client.FileSystemName);

                using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethods.Put, client.PrimaryCredentials, convention)))
                {
                    try
                    {
                        await request.WriteWithObjectAsync(filesystemDocument).ConfigureAwait(false);
                    }
                    catch (ErrorResponseException e)
                    {
                        if (e.StatusCode == HttpStatusCode.Conflict)
                            throw new InvalidOperationException("Cannot create file system with the name '" + newFileSystemName + "' because it already exists. Use CreateOrUpdateFileSystemAsync in case you want to update an existing file system", e).SimplifyException();

                        throw;
                    }
                    catch (Exception e)
                    {
                        throw e.SimplifyException();
                    }
                }
            }

            public async Task CreateOrUpdateFileSystemAsync(FileSystemDocument filesystemDocument, string newFileSystemName = null)
            {
                var requestUriString = string.Format("{0}/admin/fs/{1}?update=true", client.ServerUrl,
                                                     newFileSystemName ?? client.FileSystemName);

                using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethods.Put, client.PrimaryCredentials, convention)))
                {
                    try
                    {
                        await request.WriteWithObjectAsync(filesystemDocument).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        throw e.SimplifyException();
                    }
                }
            }

            public async Task DeleteFileSystemAsync(string fileSystemName = null, bool hardDelete = false)
            {
                var requestUriString = string.Format("{0}/admin/fs/{1}?hard-delete={2}", client.ServerUrl, fileSystemName ?? client.FileSystemName, hardDelete);

                using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethods.Delete, client.PrimaryCredentials, convention)))
                {
                    try
                    {
                        await request.ExecuteRequestAsync().ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        throw e.SimplifyException();
                    }
                }
            }

            public async Task EnsureFileSystemExistsAsync(string fileSystem)
            {
                var filesystems = await GetNamesAsync().ConfigureAwait(false);
                if (filesystems.Contains(fileSystem))
                    return;

                await CreateOrUpdateFileSystemAsync(MultiDatabase.CreateFileSystemDocument(fileSystem), fileSystem).ConfigureAwait(false);
            }

            public async Task<long> StartRestore(FilesystemRestoreRequest restoreRequest)
            {
                var requestUrlString = string.Format("{0}/admin/fs-restore", client.ServerUrl);

                using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUrlString, HttpMethods.Post, client.PrimaryCredentials, convention)))
                {
                    try
                    {
                        await request.WriteWithObjectAsync(restoreRequest).ConfigureAwait(false);

                        var response = await request.ReadResponseJsonAsync().ConfigureAwait(false);
                        return response.Value<long>("OperationId");
                    }
                    catch (Exception e)
                    {
                        throw e.SimplifyException();
                    }
                }
            }

            public async Task<long> StartCompact(string filesystemName)
            {
                var requestUrlString = string.Format("{0}/admin/fs-compact?filesystem={1}", client.ServerUrl, Uri.EscapeDataString(filesystemName));

                using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUrlString, HttpMethods.Post, client.PrimaryCredentials, convention)))
                {
                    try
                    {
                        var response = await request.ReadResponseJsonAsync().ConfigureAwait(false);
                        return response.Value<long>("OperationId");
                    }
                    catch (Exception e)
                    {
                        throw e.SimplifyException();
                    }
                }
            }

            public async Task<long> StartBackup(string backupLocation, FileSystemDocument fileSystemDocument, bool incremental, string fileSystemName)
            {
                var requestUrlString = string.Format("{0}/fs/{1}/admin/backup?incremental={2}", client.ServerUrl, fileSystemName, incremental);

                using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUrlString, HttpMethods.Post, client.PrimaryCredentials, convention)))
                {
                    try
                    {
                        await request.WriteWithObjectAsync(new FilesystemBackupRequest
                        {
                            BackupLocation = backupLocation,
                            FileSystemDocument = fileSystemDocument
                        }).ConfigureAwait(false);

                        var response = await request.ReadResponseJsonAsync().ConfigureAwait(false);
                        return response.Value<long>("OperationId");
                    }
                    catch (Exception e)
                    {
                        throw e.SimplifyException();
                    }
                }
            }

            public async Task ResetIndexes(string filesystemName)
            {
                var requestUrlString = string.Format("{0}/fs/{1}/admin/reset-index", client.ServerUrl, Uri.EscapeDataString(filesystemName));

                using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUrlString, HttpMethods.Post, client.PrimaryCredentials, convention)))
                {
                    try
                    {
                        await request.ExecuteRequestAsync().ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        throw e.SimplifyException();
                    }
                }
            }

            public ProfilingInformation ProfilingInformation { get; private set; }

            public void Dispose()
            {
            }
        }

        public ProfilingInformation ProfilingInformation { get; private set; }
    }
}
