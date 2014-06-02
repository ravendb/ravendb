using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.OAuth;
using Raven.Abstractions.RavenFS;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Connection.Profiling;
using Raven.Client.FileSystem;
using Raven.Client.FileSystem.Changes;
using Raven.Client.FileSystem.Connection;
using Raven.Client.FileSystem.Extensions;
using Raven.Client.RavenFS;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem
{

    public class AsyncFilesServerClient : AsyncServerClientBase<FilesConvention, IFilesReplicationInformer>, IDisposable, IHoldProfilingInformation
    {
        private readonly FilesChangesClient notifications;

        private IDisposable failedUploadsObserver;

        private const int DefaultNumberOfCachedRequests = 2048;
        private static HttpJsonRequestFactory GetHttpJsonRequestFactory ()
        {
#if !NETFX_CORE
              return new HttpJsonRequestFactory(DefaultNumberOfCachedRequests);
#else
			  return new HttpJsonRequestFactory();
#endif
        }

        private readonly ConcurrentDictionary<Guid, CancellationTokenSource> uploadCancellationTokens = new ConcurrentDictionary<Guid, CancellationTokenSource>();

        /// <summary>
        /// Notify when the failover status changed
        /// </summary>
        public event EventHandler<FailoverStatusChangedEventArgs> FailoverStatusChanged
        {
            add { this.ReplicationInformer.FailoverStatusChanged += value; }
            remove { this.ReplicationInformer.FailoverStatusChanged -= value; }
        }


        public AsyncFilesServerClient(string serverUrl, string fileSystemName, ICredentials credentials = null, string apiKey = null)
            : base(serverUrl, new FilesConvention(), new OperationCredentials(apiKey, credentials ?? CredentialCache.DefaultNetworkCredentials), GetHttpJsonRequestFactory(), null, new NameValueCollection())
        {
            try
            {
                FileSystemName = fileSystemName;
                ApiKey = apiKey;

                notifications = new FilesChangesClient(serverUrl, apiKey, credentials, RequestFactory, this.Convention, this.ReplicationInformer, () => { });
                               
                InitializeSecurity();
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }
        }

        protected override IFilesReplicationInformer GetReplicationInformer()
        {
            return new FilesReplicationInformer(this.Convention, this.RequestFactory);
        }

        public override string BaseUrl
        {
            get { return this.ServerUrl + "/fs/" + this.FileSystemName; }
        }

        public string FileSystemName { get; private set; }

        public string ApiKey { get; private set; }

        public bool IsObservingFailedUploads
        {
            get { return failedUploadsObserver != null; }
            set
            {
                if (value)
                {
                    failedUploadsObserver = notifications.ForCancellations()
                                                         .Subscribe(CancelFileUpload);
                }
                else
                {
                    failedUploadsObserver.Dispose();
                    failedUploadsObserver = null;
                }
            }
        }

        private void InitializeSecurity()
        {
            if (Convention.HandleUnauthorizedResponseAsync != null)
                return; // already setup by the user

            var basicAuthenticator = new BasicAuthenticator(RequestFactory.EnableBasicAuthenticationOverUnsecuredHttpEvenThoughPasswordsWouldBeSentOverTheWireInClearTextToBeStolenByHackers);
            var securedAuthenticator = new SecuredAuthenticator();

            RequestFactory.ConfigureRequest += basicAuthenticator.ConfigureRequest;
            RequestFactory.ConfigureRequest += securedAuthenticator.ConfigureRequest;

            Convention.HandleForbiddenResponseAsync = (forbiddenResponse, credentials) =>
            {
                if (credentials.ApiKey == null)
                {
                    AssertForbiddenCredentialSupportWindowsAuth(forbiddenResponse);
                    return null;
                }

                return null;
            };

            Convention.HandleUnauthorizedResponseAsync = (unauthorizedResponse, credentials) =>
            {
                var oauthSource = unauthorizedResponse.Headers.GetFirstValue("OAuth-Source");

#if DEBUG && FIDDLER
                // Make sure to avoid a cross DNS security issue, when running with Fiddler
				if (string.IsNullOrEmpty(oauthSource) == false)
					oauthSource = oauthSource.Replace("localhost:", "localhost.fiddler:");
#endif

                // Legacy support
                if (string.IsNullOrEmpty(oauthSource) == false &&
                    oauthSource.EndsWith("/OAuth/API-Key", StringComparison.CurrentCultureIgnoreCase) == false)
                {
                    return basicAuthenticator.HandleOAuthResponseAsync(oauthSource, credentials.ApiKey);
                }

                if (credentials.ApiKey == null)
                {
                    AssertUnauthorizedCredentialSupportWindowsAuth(unauthorizedResponse, credentials.Credentials);
                    return null;
                }

                if (string.IsNullOrEmpty(oauthSource))
                    oauthSource = ServerUrl + "/OAuth/API-Key";

                return securedAuthenticator.DoOAuthRequestAsync(ServerUrl, oauthSource, credentials.ApiKey);
            };

        }

        private void AssertForbiddenCredentialSupportWindowsAuth(HttpResponseMessage response)
        {
            if (PrimaryCredentials.Credentials == null)
                return;

            var requiredAuth = response.Headers.GetFirstValue("Raven-Required-Auth");
            if (requiredAuth == "Windows")
            {
                // we are trying to do windows auth, but we didn't get the windows auth headers
                throw new SecurityException(
                    "Attempted to connect to a RavenDB Server that requires authentication using Windows credentials, but the specified server does not support Windows authentication." +
                    Environment.NewLine +
                    "If you are running inside IIS, make sure to enable Windows authentication.");
            }
        }

        private void AssertUnauthorizedCredentialSupportWindowsAuth(HttpResponseMessage response, ICredentials credentials)
        {
            if (credentials == null)
                return;

            var authHeaders = response.Headers.WwwAuthenticate.FirstOrDefault();
            if (authHeaders == null ||
                (authHeaders.ToString().Contains("NTLM") == false && authHeaders.ToString().Contains("Negotiate") == false)
                )
            {
                // we are trying to do windows auth, but we didn't get the windows auth headers
                throw new SecurityException(
                    "Attempted to connect to a RavenDB Server that requires authentication using Windows credentials," + Environment.NewLine
                    + " but either wrong credentials where entered or the specified server does not support Windows authentication." +
                    Environment.NewLine +
                    "If you are running inside IIS, make sure to enable Windows authentication.");
            }
        }

        public Task<FileSystemStats> StatsAsync()
        {
            return ExecuteWithReplication("GET", async operation =>
            {
                var requestUriString = operation.Url + "/stats";
                var request =
                    RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString,
                        "GET", operation.Credentials, Convention));

                try
                {
                    var response = (RavenJObject) await request.ReadResponseJsonAsync();
                    return JsonExtensions.JsonDeserialization<FileSystemStats>(response);
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            });
        }

        public Task DeleteAsync(string filename)
        {
            return ExecuteWithReplication("DELETE", async operation =>
            {
                var requestUriString = operation.Url + "/files/" + Uri.EscapeDataString(filename);

                var request = RequestFactory.CreateHttpJsonRequest(
                                        new CreateHttpJsonRequestParams(this, requestUriString,
                                                                        "DELETE", operation.Credentials, Convention));

                try
                {
                    await request.ExecuteRequestAsync();
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            });
        }

        public Task RenameAsync(string filename, string rename)
        {
            return ExecuteWithReplication("PATCH", async operation =>
            {
                var requestUriString = operation.Url + "/files/" + Uri.EscapeDataString(filename) + "?rename=" + Uri.EscapeDataString(rename);

                var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString,
                                                                                                       "PATCH", operation.Credentials, Convention));

                try
                {
                    await request.ExecuteRequestAsync();
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            });
        }

        public Task<FileHeader[]> BrowseAsync(int start = 0, int pageSize = 25)
        {
            return ExecuteWithReplication("GET", async operation =>
            {
                var request = RequestFactory.CreateHttpJsonRequest(
                                    new CreateHttpJsonRequestParams(this, operation.Url + "/files?start=" + start + "&pageSize=" + pageSize,
                                                                    "GET", operation.Credentials, Convention));

                try
                {
                    var response = (RavenJArray) await request.ReadResponseJsonAsync();
                    var items = response.Select(x => JsonExtensions.JsonDeserialization<FileHeader>((RavenJObject)x));
                    return items.ToArray();
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            });
        }

        public Task<string[]> GetSearchFieldsAsync(int start = 0, int pageSize = 25)
        {
            return ExecuteWithReplication("GET", async operation =>
            {
                var requestUriString = string.Format("{0}/search/terms?start={1}&pageSize={2}", operation.Url, start, pageSize);
                var request = RequestFactory.CreateHttpJsonRequest(
                                        new CreateHttpJsonRequestParams(this, requestUriString,
                                                                        "GET", operation.Credentials, Convention));

                try
                {
                    var response = (RavenJArray) await request.ReadResponseJsonAsync();
                    var items = response.Select(x => x.Value<string>());
                    return items.ToArray();
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            });
        }

        public Task<SearchResults> SearchAsync(string query, string[] sortFields = null, int start = 0, int pageSize = 25)
        {
            return ExecuteWithReplication("GET", async operation =>
            {
                var requestUriBuilder = new StringBuilder(operation.Url)
                    .Append("/search/?query=")
                    .Append(Uri.EscapeUriString(query))
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

                var request = RequestFactory.CreateHttpJsonRequest(
                                                        new CreateHttpJsonRequestParams(this, requestUriBuilder.ToString(),
                                                                                        "GET", operation.Credentials, Convention));
                try
                {
                    var response = (RavenJObject)await request.ReadResponseJsonAsync();        
                    
                    return JsonExtensions.JsonDeserialization<SearchResults>(response);
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            });

        }

        public Task<RavenJObject> GetMetadataForAsync(string filename)
        {
            return ExecuteWithReplication("HEAD", operation => GetMetadataForAsyncImpl(filename, operation));
        }

        private async Task<RavenJObject> GetMetadataForAsyncImpl(string filename, OperationMetadata operation)
        {
            var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operation.Url + "/files?name=" + Uri.EscapeDataString(filename),
                                                                                                   "HEAD", operation.Credentials, Convention));

            try
            {                
                var response = await request.ReadResponseJsonAsync();

                var metadata = request.ResponseHeaders.HeadersToObject();
                metadata["etag"] = new RavenJValue(Guid.Parse(request.ResponseHeaders["ETag"].Trim('\"')));
                return metadata;
            }
            catch (Exception e)
            {
                var aggregateException = e as AggregateException;

                var responseException = e as ErrorResponseException;
                if (responseException == null && aggregateException != null)
                    responseException = aggregateException.ExtractSingleInnerException() as ErrorResponseException;
                if (responseException != null)
                {
                    if (responseException.StatusCode == HttpStatusCode.NotFound)
                        return null;
                    throw e.SimplifyException();
                }

                throw e.SimplifyException();
            }
        }

        public Task<RavenJObject> DownloadAsync(string filename, Stream destination, long? from = null, long? to = null)
        {
            return DownloadAsync("/files/", filename, destination, from, to);
        }

        private Task<RavenJObject> DownloadAsync(string path, string filename, Stream destination,
                                                              long? from = null, long? to = null,
                                                              Action<string, long> progress = null)
        {
            return ExecuteWithReplication("GET", operation => DownloadAsyncImpl(path, filename, destination, @from, to, progress, operation));

        }

        private async Task<RavenJObject> DownloadAsyncImpl(string path, string filename, Stream destination, long? @from, long? to, Action<string, long> progress, OperationMetadata operation)
        {
            var collection = new RavenJObject();
            if (destination.CanWrite == false)
                throw new ArgumentException("Stream does not support writing");

            var request =
                RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operation.Url + path + filename,
                                                                                         "GET", operation.Credentials, Convention));

            if (@from != null)
            {
                if (to != null)
                    request.AddRange(@from.Value, to.Value);
                else
                    request.AddRange(@from.Value);
            }
            else if (destination.CanSeek)
            {
                destination.Position = destination.Length;
                request.AddRange(destination.Position);
            }

            try
            {
                using (var responseStream = new MemoryStream(await request.ReadResponseBytesAsync()))
                {
                    foreach (var header in request.ResponseHeaders.AllKeys)
                    {
                        collection[header] = request.ResponseHeaders[header];
                    }
                    await responseStream.CopyToAsync(destination, i =>
                    {
                        if (progress != null)
                            progress(filename, i);
                    });
                }

                return collection;
            }
            catch (Exception e)
            {
                throw e.SimplifyException();
            }
        }


        public Task UpdateMetadataAsync(string filename, RavenJObject metadata)
        {
            return ExecuteWithReplication("POST", async operation =>
            {
                var request = RequestFactory.CreateHttpJsonRequest(
                                    new CreateHttpJsonRequestParams(this, operation.Url + "/files/" + filename,
                                                                    "POST", operation.Credentials, Convention));

                AddHeaders(metadata, request);

                try
                {
                    await request.ExecuteRequestAsync();
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            });
        }

        public Task UploadAsync(string filename, Stream source)
        {
            return UploadAsync(filename, new RavenJObject(), source, null);
        }


        public Task UploadAsync(string filename, RavenJObject metadata, Stream source)
        {
            return UploadAsync(filename, metadata, source, null);
        }

        public Task UploadAsync(string filename, RavenJObject metadata, Stream source, Action<string, long> progress)
        {
            return ExecuteWithReplication("PUT", async operation =>
            {
                if (source.CanRead == false)
                    throw new Exception("Stream does not support reading");

                var uploadIdentifier = Guid.NewGuid();
                var request = RequestFactory.CreateHttpJsonRequest(
                                new CreateHttpJsonRequestParams(this, operation.Url + "/files?name=" + Uri.EscapeDataString(filename) + "&uploadId=" + uploadIdentifier,
                                                                "PUT", operation.Credentials, Convention));

                metadata["RavenFS-Size"] = new RavenJValue(source.Length);

                AddHeaders(metadata, request);

                var cts = new CancellationTokenSource();

                RegisterUploadOperation(uploadIdentifier, cts);

                try
                {
                    await request.WriteAsync(source);
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
                finally
                {
                    UnregisterUploadOperation(uploadIdentifier);
                }
            });
        }

        private void CancelFileUpload(CancellationNotification uploadFailed)
        {
            CancellationTokenSource cts;
            if (uploadCancellationTokens.TryGetValue(uploadFailed.UploadId, out cts))
            {
                cts.Cancel();
            }
        }

        private void RegisterUploadOperation(Guid uploadId, CancellationTokenSource cts)
        {
            if (IsObservingFailedUploads)
                uploadCancellationTokens.TryAdd(uploadId, cts);
        }

        private void UnregisterUploadOperation(Guid uploadId)
        {
            if (IsObservingFailedUploads)
            {
                CancellationTokenSource cts;
                uploadCancellationTokens.TryRemove(uploadId, out cts);
            }
        }

        public SynchronizationClient Synchronization
        {
            get
            {
                return new SynchronizationClient(this, Convention);
            }
        }

        public ConfigurationClient Config
        {
            get { return new ConfigurationClient(this, Convention); }
        }

        public StorageClient Storage
        {
            get
            {
                return new StorageClient(this, Convention);
            }
        }

        public AdminClient Admin
        {
            get
            {
                return new AdminClient(this, Convention);
            }
        }

        /// <summary>
        /// Subscribe to change notifications from the server
        /// </summary>
        public IFilesChanges Changes()
        {
            return this.notifications;
        }

        private static void AddHeaders(RavenJObject metadata, HttpJsonRequest request)
        {
            foreach( var item in metadata )
            {
	            var value = item.Value is RavenJValue ? item.Value.ToString() : item.Value.ToString(Formatting.None);
				request.AddHeader(item.Key, value); 
            }
        }

        public Task<string[]> GetFoldersAsync(string from = null, int start = 0, int pageSize = 25)
        {
            return ExecuteWithReplication("GET", async operation =>
            {
                var path = @from ?? "";
                if (path.StartsWith("/"))
                    path = path.Substring(1);

                var requestUriString = operation.Url + "/folders/subdirectories/" + Uri.EscapeUriString(path) + "?pageSize=" +
                                       pageSize + "&start=" + start;

                var request =
                    RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString,
                        "GET", operation.Credentials, Convention));

                try
                {
                    var response = await request.ReadResponseJsonAsync();

                    return new JsonSerializer().Deserialize<string[]>(new RavenJTokenReader(response));
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            });
        }

        public Task<SearchResults> GetFilesAsync(string folder, FilesSortOptions options = FilesSortOptions.Default,
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

        public Task<Guid> GetServerId()
        {
            return ExecuteWithReplication("GET", async operation =>
            {
                var requestUriString = operation.Url + "/static/id";

                var request = RequestFactory.CreateHttpJsonRequest(
                                        new CreateHttpJsonRequestParams(this, requestUriString, "GET", operation.Credentials, Convention));

                try
                {
                    var response = await request.ReadResponseJsonAsync();
                    return new JsonSerializer().Deserialize<Guid>(new RavenJTokenReader(response));
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
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

            var folderQueryPart = "__directory:" + folder + " AND __level:" + level;
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
                    throw new ArgumentException("options");
                sort = "-" + sort;
            }

            var sortFields = string.IsNullOrEmpty(sort) ? null : new[] { sort };
            return sortFields;
        }

        public class ConfigurationClient : IHoldProfilingInformation
        {
            private readonly AsyncFilesServerClient ravenFileSystemClient;
            private readonly FilesConvention convention;
            private readonly JsonSerializer jsonSerializer;
            private readonly string filesystemName;

            public ConfigurationClient(AsyncFilesServerClient ravenFileSystemClient, FilesConvention convention)
            {
                this.jsonSerializer = new JsonSerializer();
                this.ravenFileSystemClient = ravenFileSystemClient;
                this.convention = convention;
                this.filesystemName = ravenFileSystemClient.FileSystemName;
            }

            public Task<string[]> GetConfigNames(int start = 0, int pageSize = 25)
            {
                return ravenFileSystemClient.ExecuteWithReplication("GET", async operation =>
                {
                    var requestUriString = operation.Url + "/config?start=" + start + "&pageSize=" + pageSize;

                    var request =
                        ravenFileSystemClient.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString,
                            "GET", operation.Credentials, convention));

                    try
                    {
                        var response = await request.ReadResponseJsonAsync();
                        return jsonSerializer.Deserialize<string[]>(new RavenJTokenReader(response));
                    }
                    catch (Exception e)
                    {
                        throw e.SimplifyException();
                    }
                });
            }

            public Task SetConfig<T>(string name, T data)
            {
                return ravenFileSystemClient.ExecuteWithReplication("PUT", async operation =>
                {
                    var requestUriString = operation.Url + "/config?name=" + Uri.EscapeDataString(name);
                    var request = ravenFileSystemClient.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "PUT", operation.Credentials, convention));

                    var jsonData = data as RavenJObject;
                    if (jsonData != null)
                    {
                        await request.WriteAsync(jsonData);
                    }
                    else if ( data is NameValueCollection)
                    {
                        throw new ArgumentException("NameValueCollection objects are not supported to be stored in RavenFS configuration");
                    }
                    else
                    {
                        await request.WriteAsync(JsonExtensions.ToJObject(data));
                    }
                });
            }

            public Task SetDestinationsConfig(params SynchronizationDestination[] destinations)
            {
                return ravenFileSystemClient.ExecuteWithReplication("PUT", async operation =>
                {
                    var requestUriString = operation.Url + "/config?name=" + Uri.EscapeDataString(SynchronizationConstants.RavenSynchronizationDestinations);
                    var request = ravenFileSystemClient.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "PUT", operation.Credentials, convention));

                    var data = new { Destinations = destinations };

                    using (var ms = new MemoryStream())
                    using (var streamWriter = new StreamWriter(ms))
                    {
                        jsonSerializer.Serialize(streamWriter, data);
                        streamWriter.Flush();
                        ms.Position = 0;
                        await request.WriteAsync(ms);
                    }
                });
            }

            public Task DeleteConfig(string name)
            {
                return ravenFileSystemClient.ExecuteWithReplication("DELETE", operation =>
                {
                    var requestUriString = operation.Url + "/config?name=" + Uri.EscapeDataString(name);

                    var request = ravenFileSystemClient.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "DELETE", operation.Credentials, convention));

                    return request.ExecuteRequestAsync();
                });
            }

            public Task<T> GetConfig<T>(string name)
            {
                return ravenFileSystemClient.ExecuteWithReplication("GET", async operation =>
                {
                    var requestUriString = operation.Url + "/config?name=" + Uri.EscapeDataString(name);

                    var request = ravenFileSystemClient.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "GET", operation.Credentials, convention));

                    try
                    {
                        var response = (RavenJObject) await request.ReadResponseJsonAsync();
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
                });
            }

            public Task<ConfigSearchResults> SearchAsync(string prefix, int start = 0, int pageSize = 25)
            {
                return ravenFileSystemClient.ExecuteWithReplication("GET", async operation =>
                {
                    var requestUriBuilder = new StringBuilder(operation.Url)
                        .Append("/config/search/?prefix=")
                        .Append(Uri.EscapeUriString(prefix))
                        .Append("&start=")
                        .Append(start)
                        .Append("&pageSize=")
                        .Append(pageSize);

                    var request =
                    ravenFileSystemClient.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriBuilder.ToString(),
                        "GET", operation.Credentials, convention));

                    try
                    {
                        var response = await request.ReadResponseJsonAsync();
                        using (var jsonTextReader = new RavenJTokenReader(response))
                        {
                            return jsonSerializer.Deserialize<ConfigSearchResults>(jsonTextReader);
                        }
                    }
                    catch (Exception e)
                    {
                        throw e.SimplifyException();
                    }
                });
            }

            public ProfilingInformation ProfilingInformation { get; private set; }
        }

        public class SynchronizationClient : IHoldProfilingInformation
        {
            private readonly OperationCredentials credentials;
            private readonly FilesConvention convention;
            private readonly HttpJsonRequestFactory jsonRequestFactory;
            private readonly AsyncFilesServerClient fullClient;

            public SynchronizationClient(AsyncFilesServerClient client, FilesConvention convention)
            {
                this.credentials = client.PrimaryCredentials;
                this.jsonRequestFactory = client.RequestFactory;
                this.convention = convention;
                this.BaseUrl = client.BaseUrl;
                this.fullClient = client;
            }

            public string BaseUrl { get; private set; }

            public FilesConvention Convention
            {
                get { return convention; }
            }

            public OperationCredentials Credentials
            {
                get { return credentials; }
            }

            public HttpJsonRequestFactory RequestFactory
            {
                get { return jsonRequestFactory; }
            }

            public Task<RavenJObject> GetMetadataForAsync(string filename)
            {
                return fullClient.GetMetadataForAsyncImpl(filename, new OperationMetadata(BaseUrl, credentials));
            }

            public Task DownloadSignatureAsync(string sigName, Stream destination, long? from = null, long? to = null)
            {
                return fullClient.DownloadAsyncImpl("/rdc/signatures/", sigName, destination, from, to, null, new OperationMetadata(BaseUrl, credentials));
            }

            public async Task<SignatureManifest> GetRdcManifestAsync(string path)
            {
                var requestUriString = BaseUrl + "/rdc/manifest/" + Uri.EscapeDataString(path);
                var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString,
                                                                                                       "GET", credentials, convention));

                try
                {
                    var response = (RavenJObject) await request.ReadResponseJsonAsync();
                    return response.JsonDeserialization<SignatureManifest>();
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            }

            public async Task<DestinationSyncResult[]> SynchronizeDestinationsAsync(bool forceSyncingAll = false)
            {
                var requestUriString = String.Format("{0}/synchronization/ToDestinations?forceSyncingAll={1}", BaseUrl, forceSyncingAll);

                var request = RequestFactory.CreateHttpJsonRequest(
                                                    new CreateHttpJsonRequestParams(this, requestUriString,
                                                                                    "POST", credentials, convention));

                try
                {
                    var response = (RavenJArray) await request.ReadResponseJsonAsync();
                    return response.Select(x => ((RavenJObject)x).JsonDeserialization<DestinationSyncResult>()).ToArray();
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            }

            public Task<SynchronizationReport> StartAsync(string fileName, AsyncFilesServerClient destination)
            {
                return StartAsync(fileName, destination.ToSynchronizationDestination());
            }

            public async Task<SynchronizationReport> StartAsync(string fileName, SynchronizationDestination destination)
            {
                var requestUriString = String.Format("{0}/synchronization/start/{1}", BaseUrl, Uri.EscapeDataString(fileName));

                var request = RequestFactory.CreateHttpJsonRequest(
                                                    new CreateHttpJsonRequestParams(this, requestUriString,
                                                                                    "POST", credentials, convention));

                try
                {
                    await request.WriteAsync(JsonExtensions.ToJObject(destination));
                    var response = (RavenJObject)await request.ReadResponseJsonAsync();
                    return response.JsonDeserialization<SynchronizationReport>();
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            }

            public async Task<SynchronizationReport> GetSynchronizationStatusAsync(string fileName)
            {
                var requestUriString = String.Format("{0}/synchronization/status/{1}", BaseUrl, Uri.EscapeDataString(fileName));

                var request = RequestFactory.CreateHttpJsonRequest(
                                                    new CreateHttpJsonRequestParams(this, requestUriString,
                                                                                    "GET", credentials, convention));

                try
                {
                    var response = (RavenJObject)await request.ReadResponseJsonAsync();
                    return response.JsonDeserialization<SynchronizationReport>();
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            }

            public async Task ResolveConflictAsync(string filename, ConflictResolutionStrategy strategy)
            {
                var requestUriString = String.Format("{0}/synchronization/resolveConflict/{1}?strategy={2}",
                                                        BaseUrl, Uri.EscapeDataString(filename),
                                                        Uri.EscapeDataString(strategy.ToString()));

                var request = RequestFactory.CreateHttpJsonRequest(
                                    new CreateHttpJsonRequestParams(this, requestUriString,
                                                                    "PATCH", credentials, convention));

                try
                {
                    await request.ExecuteRequestAsync();
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            }

            public async Task ApplyConflictAsync(string filename, long remoteVersion, string remoteServerId,
                                                   IList<HistoryItem> remoteHistory, string remoteServerUrl)
            {
                var requestUriString =
                    String.Format("{0}/synchronization/applyConflict/{1}?remoteVersion={2}&remoteServerId={3}&remoteServerUrl={4}",
                                  BaseUrl, Uri.EscapeDataString(filename), remoteVersion,
                                  Uri.EscapeDataString(remoteServerId), Uri.EscapeDataString(remoteServerUrl));

                var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString,
                        "PATCH", credentials, convention));

                try
                {
                    using (var stream = new MemoryStream())
                    {
                        var sb = new StringBuilder();
                        var jw = new JsonTextWriter(new StringWriter(sb));
                        new JsonSerializer().Serialize(jw, remoteHistory);
                        var bytes = Encoding.UTF8.GetBytes(sb.ToString());

                        await stream.WriteAsync(bytes, 0, bytes.Length);
                        stream.Position = 0;
                        await request.WriteAsync(stream);
                    }
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            }

            public async Task<ListPage<SynchronizationReport>> GetFinishedAsync(int page = 0, int pageSize = 25)
            {
                var requestUriString = String.Format("{0}/synchronization/finished?start={1}&pageSize={2}", BaseUrl, page,
                                                         pageSize);

                var request = RequestFactory.CreateHttpJsonRequest(
                                        new CreateHttpJsonRequestParams(this, requestUriString,
                                                                        "GET", credentials, convention));

                try
                {
                    var response = await request.ReadResponseJsonAsync();
                    var preResult =
                        new JsonSerializer().Deserialize<ListPage<SynchronizationReport>>(new RavenJTokenReader(response));
                    return preResult;
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            }

            public async Task<ListPage<SynchronizationDetails>> GetActiveAsync(int page = 0, int pageSize = 25)
            {
                var requestUriString = String.Format("{0}/synchronization/active?start={1}&pageSize={2}",
                                                        BaseUrl, page, pageSize);

                var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString,
                        "GET", credentials, convention));

                try
                {
                    var response = await request.ReadResponseJsonAsync();
                    var preResult =
                        new JsonSerializer().Deserialize<ListPage<SynchronizationDetails>>(new RavenJTokenReader(response));
                    return preResult;
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            }

            public async Task<ListPage<SynchronizationDetails>> GetPendingAsync(int page = 0, int pageSize = 25)
            {
                var requestUriString = String.Format("{0}/synchronization/pending?start={1}&pageSize={2}",
                                                     BaseUrl, page, pageSize);

                var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString,
                        "GET", credentials, convention));

                try
                {
                    var response = await request.ReadResponseJsonAsync();

                    var preResult =
                        new JsonSerializer().Deserialize<ListPage<SynchronizationDetails>>(new RavenJTokenReader(response));
                    return preResult;

                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            }

            public async Task<SourceSynchronizationInformation> GetLastSynchronizationFromAsync(Guid serverId)
            {
                var requestUriString = String.Format("{0}/synchronization/LastSynchronization?from={1}", BaseUrl, serverId);

                var request = RequestFactory.CreateHttpJsonRequest(
                                                    new CreateHttpJsonRequestParams(this, requestUriString,
                                                                                    "GET", credentials, convention));

                try
                {
                    var response = await request.ReadResponseJsonAsync();
                    var preResult = new JsonSerializer().Deserialize<SourceSynchronizationInformation>(new RavenJTokenReader(response));
                    return preResult;
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            }

            public async Task<IEnumerable<SynchronizationConfirmation>> ConfirmFilesAsync(IEnumerable<Tuple<string, Guid>> sentFiles)
            {
                var requestUriString = String.Format("{0}/synchronization/Confirm", BaseUrl);

                var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "POST", credentials, convention));

                try
                {
                    using (var stream = new MemoryStream())
                    {
                        var sb = new StringBuilder();
                        var jw = new JsonTextWriter(new StringWriter(sb));
                        new JsonSerializer().Serialize(jw, sentFiles);
                        var bytes = Encoding.UTF8.GetBytes(sb.ToString());

                        await stream.WriteAsync(bytes, 0, bytes.Length);
                        stream.Position = 0;
                        await request.WriteAsync(stream);
                        var response = await request.ReadResponseJsonAsync();

                        return new JsonSerializer().Deserialize<IEnumerable<SynchronizationConfirmation>>(
                            new RavenJTokenReader(response));
                    }

                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            }

            public async Task<ListPage<ConflictItem>> GetConflictsAsync(int page = 0, int pageSize = 25)
            {
                var requestUriString = String.Format("{0}/synchronization/conflicts?start={1}&pageSize={2}", BaseUrl, page,
                                                         pageSize);

                var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString,
                        "GET", credentials, convention));

                try
                {
                    var response = (RavenJObject)await request.ReadResponseJsonAsync();
                    return response.JsonDeserialization<ListPage<ConflictItem>>();
                    //var preResult =
                    //    new JsonSerializer().Deserialize<ListPage<ConflictItem>>(new RavenJTokenReader(response));
                    //return preResult;
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            }

            public async Task IncrementLastETagAsync(Guid sourceServerId, string sourceFileSystemUrl, Guid sourceFileETag)
            {
                var requestUriString =
                    String.Format("{0}/synchronization/IncrementLastETag?sourceServerId={1}&sourceFileSystemUrl={2}&sourceFileETag={3}",
                                    BaseUrl, sourceServerId, sourceFileSystemUrl, sourceFileETag);

                var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString,
                        "POST", credentials, convention));

                try
                {
                    await request.ExecuteRequestAsync();
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            }

            public async Task<RdcStats> GetRdcStatsAsync()
            {
                var requestUriString = BaseUrl + "/rdc/stats";

                var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString,
                        "GET", credentials, convention));

                try
                {
                    var response = await request.ReadResponseJsonAsync();
                    return new JsonSerializer().Deserialize<RdcStats>(new RavenJTokenReader(response));
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            }

            public async Task<SynchronizationReport> RenameAsync(string currentName, string newName, RavenJObject currentMetadata, ServerInfo sourceServer)
            {
                var request = RequestFactory.CreateHttpJsonRequest(
                                        new CreateHttpJsonRequestParams(this, BaseUrl + "/synchronization/rename?filename=" + Uri.EscapeDataString(currentName) + "&rename=" +
                                                                        Uri.EscapeDataString(newName), "PATCH", credentials, convention));

                request.AddHeaders(currentMetadata);
                request.AddHeader(SyncingMultipartConstants.SourceServerInfo, sourceServer.AsJson());

                try
                {
                    var response = await request.ReadResponseJsonAsync();
                    return new JsonSerializer().Deserialize<SynchronizationReport>(new RavenJTokenReader(response));
                }
                catch (ErrorResponseException exception)
                {
                    throw exception.SimplifyException();
                }
            }

            public async Task<SynchronizationReport> DeleteAsync(string fileName, RavenJObject metadata, ServerInfo sourceServer)
            {
                var request = RequestFactory.CreateHttpJsonRequest(
                                    new CreateHttpJsonRequestParams(this, BaseUrl + "/synchronization?fileName=" + Uri.EscapeDataString(fileName),
                                                                    "DELETE", credentials, convention));

                request.AddHeaders(metadata);
                request.AddHeader(SyncingMultipartConstants.SourceServerInfo, sourceServer.AsJson());

                try
                {
                    var response = await request.ReadResponseJsonAsync();
                    return new JsonSerializer().Deserialize<SynchronizationReport>(new RavenJTokenReader(response));
                }
                catch (ErrorResponseException exception)
                {
                    throw exception.SimplifyException();
                }
            }

            public async Task<SynchronizationReport> UpdateMetadataAsync(string fileName, RavenJObject metadata, ServerInfo sourceServer)
            {
                // REVIEW: (Oren) The ETag is always rewritten by this method as If-None-Match. Maybe a convention from the Database, but found it quite difficult to debug.  
                var request = RequestFactory.CreateHttpJsonRequest(
                                    new CreateHttpJsonRequestParams(this, BaseUrl + "/synchronization/UpdateMetadata/" + Uri.EscapeDataString(fileName),
                                                                    "POST", credentials, convention));

                request.AddHeaders(metadata);
                request.AddHeader(SyncingMultipartConstants.SourceServerInfo, sourceServer.AsJson());
                // REVIEW: (Oren) and also causes this.
                request.AddHeader("ETag", "\"" + metadata.Value<string>("ETag") + "\"");

                try
                {
                    var response = await request.ReadResponseJsonAsync();
                    return new JsonSerializer().Deserialize<SynchronizationReport>(new RavenJTokenReader(response));
                }
                catch (ErrorResponseException exception)
                {
                    throw exception.SimplifyException();
                }
            }

            public ProfilingInformation ProfilingInformation { get; private set; }
        }

        public class StorageClient : IHoldProfilingInformation
        {
            private readonly AsyncFilesServerClient ravenFileSystemClient;
            private readonly FilesConvention convention;
            private readonly string filesystemName;

            public StorageClient(AsyncFilesServerClient ravenFileSystemClient, FilesConvention convention)
            {
                this.ravenFileSystemClient = ravenFileSystemClient;
                this.convention = convention;
                this.filesystemName = ravenFileSystemClient.FileSystemName;
            }

            public Task CleanUp()
            {
                return ravenFileSystemClient.ExecuteWithReplication("POST", async operation =>
                {
                    var requestUriString = String.Format("{0}/storage/cleanup", operation.Url);

                    var request = ravenFileSystemClient.RequestFactory.CreateHttpJsonRequest(
                                            new CreateHttpJsonRequestParams(this, requestUriString,
                                                                            "POST", operation.Credentials, convention));

                    try
                    {
                        await request.ExecuteRequestAsync();
                    }
                    catch (Exception e)
                    {
                        throw e.SimplifyException();
                    }
                });
            }

            public Task RetryRenaming()
            {
                return ravenFileSystemClient.ExecuteWithReplication("POST", async operation =>
                {
                    var requestUriString = String.Format("{0}/storage/retryrenaming", operation.Url);

                    var request = ravenFileSystemClient.RequestFactory.CreateHttpJsonRequest(
                                        new CreateHttpJsonRequestParams(this, requestUriString,
                                                                        "POST", operation.Credentials, convention));

                    try
                    {
                        await request.ExecuteRequestAsync();
                    }
                    catch (Exception e)
                    {
                        throw e.SimplifyException();
                    }
                });
            }

            public ProfilingInformation ProfilingInformation { get; private set; }
        }

        public class AdminClient : IHoldProfilingInformation
        {
            private readonly AsyncFilesServerClient ravenFileSystemClient;
            private readonly FilesConvention convention;
            private readonly string filesystemName;

            public AdminClient(AsyncFilesServerClient ravenFileSystemClient, FilesConvention convention)
            {
                this.ravenFileSystemClient = ravenFileSystemClient;
                this.convention = convention;
                this.filesystemName = ravenFileSystemClient.FileSystemName;
            }

            public async Task<string[]> GetFileSystemsNames()
            {
                var requestUriString = string.Format("{0}/fs/names", ravenFileSystemClient.ServerUrl);

                var request =
                    ravenFileSystemClient.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString,
                        "GET", ravenFileSystemClient.PrimaryCredentials, convention));

                try
                {
                    var response = await request.ReadResponseJsonAsync();
                    return new JsonSerializer().Deserialize<string[]>(new RavenJTokenReader(response));
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            }

            public async Task<List<FileSystemStats>> GetFileSystemsStats()
            {
                var requestUriString = string.Format("{0}/fs/stats", ravenFileSystemClient.ServerUrl);

                var request =
                    ravenFileSystemClient.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString,
                        "GET", ravenFileSystemClient.PrimaryCredentials, convention));

                try
                {
                    var response = await request.ReadResponseJsonAsync();
                    return new JsonSerializer().Deserialize<List<FileSystemStats>>(new RavenJTokenReader(response));
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            }

            public async Task CreateFileSystemAsync(DatabaseDocument databaseDocument, string newFileSystemName = null)
            {
                var requestUriString = string.Format("{0}/fs/admin/{1}", ravenFileSystemClient.ServerUrl,
                                                     newFileSystemName ?? ravenFileSystemClient.FileSystemName);

                var request = ravenFileSystemClient.RequestFactory.CreateHttpJsonRequest(
                                        new CreateHttpJsonRequestParams(this, requestUriString,
                                                                        "PUT", ravenFileSystemClient.PrimaryCredentials, convention));

                try
                {
                    await request.WriteAsync(JsonConvert.SerializeObject(databaseDocument));
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

            public async Task CreateOrUpdateFileSystemAsync(DatabaseDocument databaseDocument, string newFileSystemName = null)
            {
                var requestUriString = string.Format("{0}/fs/admin/{1}?update=true", ravenFileSystemClient.ServerUrl,
                                                     newFileSystemName ?? ravenFileSystemClient.FileSystemName);

                var request = ravenFileSystemClient.RequestFactory.CreateHttpJsonRequest(
                                        new CreateHttpJsonRequestParams(this, requestUriString,
                                                                        "PUT", ravenFileSystemClient.PrimaryCredentials, convention));

                try
                {
                    await request.WriteAsync(JsonConvert.SerializeObject(databaseDocument));                    
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            }

			public async Task DeleteFileSystemAsync(string fileSystemName = null, bool hardDelete = false)
			{
                var requestUriString = string.Format("{0}/fs/admin/{1}?hard-delete={2}", ravenFileSystemClient.ServerUrl, fileSystemName ?? ravenFileSystemClient.FileSystemName, hardDelete);

				var request = ravenFileSystemClient.RequestFactory.CreateHttpJsonRequest(
										new CreateHttpJsonRequestParams(this, requestUriString,
																		"DELETE", ravenFileSystemClient.PrimaryCredentials, convention));

				try
				{
					await request.ExecuteRequestAsync();
				}
				catch (Exception e)
				{
					throw e.SimplifyException();
				}
			}

            public ProfilingInformation ProfilingInformation { get; private set; }
        }

        public override void Dispose()
        {            
            if (notifications != null)
                notifications.Dispose();

            base.Dispose();
        }

        public ProfilingInformation ProfilingInformation { get; private set; }
    }
}