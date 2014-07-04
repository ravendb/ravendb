using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.FileSystem.Notifications;
using Raven.Abstractions.OAuth;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Connection.Profiling;
using Raven.Client.Extensions;
using Raven.Client.FileSystem.Changes;
using Raven.Client.FileSystem.Connection;
using Raven.Client.FileSystem.Extensions;
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

    public class AsyncFilesServerClient : AsyncServerClientBase<FilesConvention, IFilesReplicationInformer>,
                                          IAsyncFilesCommands, IAsyncFilesCommandsImpl,
                                          IDisposable, IHoldProfilingInformation
    {
        private readonly Lazy<FilesChangesClient> notifications;

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

        public AsyncFilesServerClient(string serverUrl, string fileSystemName, FilesConvention conventions, OperationCredentials credentials, HttpJsonRequestFactory requestFactory, Guid? sessionId, NameValueCollection operationsHeaders = null)
            : base(serverUrl, conventions, credentials, requestFactory, sessionId, operationsHeaders)
        {
            try
            {
                FileSystem = fileSystemName;                
                ApiKey = credentials.ApiKey;

                notifications = new Lazy<FilesChangesClient>( () => new FilesChangesClient(BaseUrl, ApiKey, credentials.Credentials, RequestFactory, this.Conventions, this.ReplicationInformer, () => { }));

                InitializeSecurity();
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }
        }

        public AsyncFilesServerClient(string serverUrl, string fileSystemName, ICredentials credentials = null, string apiKey = null)
            : this(serverUrl, fileSystemName, new FilesConvention(), new OperationCredentials(apiKey, credentials ?? CredentialCache.DefaultNetworkCredentials), GetHttpJsonRequestFactory(), null, new NameValueCollection())
        {
        }

        protected override IFilesReplicationInformer GetReplicationInformer()
        {
            return new FilesReplicationInformer(this.Conventions, this.RequestFactory);
        }

        protected override string BaseUrl
        {
            get { return UrlFor(); }
        }

        public override string UrlFor(string fileSystem = null)
        {
            if (string.IsNullOrWhiteSpace(fileSystem))
                fileSystem = this.FileSystem;

            return this.ServerUrl + "/fs/" + Uri.EscapeDataString(fileSystem);
        }

        public string FileSystem { get; private set; }

        public IAsyncFilesCommands ForFileSystem(string fileSystem)
        {
            return new AsyncFilesServerClient(this.ServerUrl, fileSystem, Conventions, PrimaryCredentials, RequestFactory, SessionId, OperationsHeaders);
        }

        public IAsyncFilesCommands With(ICredentials credentials)
        {
            var primaryCredentials = new OperationCredentials(this.ApiKey, credentials);
            return new AsyncFilesServerClient(this.ServerUrl, this.FileSystem, Conventions, primaryCredentials, RequestFactory, SessionId, OperationsHeaders);
        }
        public IAsyncFilesCommands With(OperationCredentials credentials)
        {
            return new AsyncFilesServerClient(this.ServerUrl, this.FileSystem, Conventions, credentials, RequestFactory, SessionId, OperationsHeaders);
        }

        public string ApiKey { get; private set; }

        public bool IsObservingFailedUploads
        {
            get { return failedUploadsObserver != null; }
            set
            {
                if (value)
                {
                    failedUploadsObserver = notifications.Value.ForCancellations()
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
            if (Conventions.HandleUnauthorizedResponseAsync != null)
                return; // already setup by the user

            var basicAuthenticator = new BasicAuthenticator(RequestFactory.EnableBasicAuthenticationOverUnsecuredHttpEvenThoughPasswordsWouldBeSentOverTheWireInClearTextToBeStolenByHackers);
            var securedAuthenticator = new SecuredAuthenticator();

            RequestFactory.ConfigureRequest += basicAuthenticator.ConfigureRequest;
            RequestFactory.ConfigureRequest += securedAuthenticator.ConfigureRequest;

            Conventions.HandleForbiddenResponseAsync = (forbiddenResponse, credentials) =>
            {
                if (credentials.ApiKey == null)
                {
                    AssertForbiddenCredentialSupportWindowsAuth(forbiddenResponse);
                    return null;
                }

                return null;
            };

            Conventions.HandleUnauthorizedResponseAsync = (unauthorizedResponse, credentials) =>
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
            if (authHeaders == null || (authHeaders.ToString().Contains("NTLM") == false && authHeaders.ToString().Contains("Negotiate") == false))
            {
                // we are trying to do windows auth, but we didn't get the windows auth headers
                throw new SecurityException(
                    "Attempted to connect to a RavenDB Server that requires authentication using Windows credentials," + Environment.NewLine
                    + " but either wrong credentials where entered or the specified server does not support Windows authentication." +
                    Environment.NewLine +
                    "If you are running inside IIS, make sure to enable Windows authentication.");
            }
        }

        public Task<FileSystemStats> GetStatisticsAsync()
        {
            return ExecuteWithReplication("GET", async operation =>
            {
                var requestUriString = operation.Url + "/stats";
                var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "GET", operation.Credentials, Conventions))
                                            .AddOperationHeaders(OperationsHeaders);

                try
                {
                    var response = (RavenJObject) await request.ReadResponseJsonAsync();
                    return response.JsonDeserialization<FileSystemStats>();
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            });
        }

        public Task DeleteAsync(string filename, Etag etag = null)
        {
            return ExecuteWithReplication("DELETE", async operation =>
            {
                var requestUriString = operation.Url + "/files/" + Uri.EscapeDataString(filename);

                var request = RequestFactory.CreateHttpJsonRequest(
                                        new CreateHttpJsonRequestParams(this, requestUriString,"DELETE", operation.Credentials, Conventions))
                                        .AddOperationHeaders(OperationsHeaders);

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

                var request = RequestFactory.CreateHttpJsonRequest(
                                                new CreateHttpJsonRequestParams(this, requestUriString,"PATCH", operation.Credentials, Conventions))
                                            .AddOperationHeaders(OperationsHeaders);

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
                                                new CreateHttpJsonRequestParams(this, operation.Url + "/files?start=" + start + "&pageSize=" + pageSize, "GET", operation.Credentials, Conventions))
                                            .AddOperationHeaders(OperationsHeaders);

                try
                {
                    var response = (RavenJArray) await request.ReadResponseJsonAsync();
                    return response.JsonDeserialization<FileHeader>();
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
                                                new CreateHttpJsonRequestParams(this, requestUriString, "GET", operation.Credentials, Conventions)
                                            .AddOperationHeaders(OperationsHeaders));

                try
                {
                    var response = (RavenJArray) await request.ReadResponseJsonAsync();
                    return response.JsonDeserialization<string>();
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
                                                 new CreateHttpJsonRequestParams(this, requestUriBuilder.ToString(),"GET", operation.Credentials, Conventions))
                                            .AddOperationHeaders(OperationsHeaders);
                try
                {
                    var response = (RavenJObject)await request.ReadResponseJsonAsync();
                    return response.JsonDeserialization<SearchResults>();
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

        public Task<FileHeader[]> GetAsync(string[] filename)
        {
            return ExecuteWithReplication("GET", operation => GetAsyncImpl(filename, operation));
        }
        private async Task<FileHeader[]> GetAsyncImpl(string[] filenames, OperationMetadata operation)
        {
            StringBuilder requestUriBuilder = new StringBuilder("/files/metadata?");
            for( int i = 0; i < filenames.Length; i++ )
            {
                requestUriBuilder.Append("fileNames=" + Uri.EscapeDataString(filenames[i]));
                if (i < filenames.Length - 1)
                    requestUriBuilder.Append("&");
            }

            var request = RequestFactory.CreateHttpJsonRequest(
                                new CreateHttpJsonRequestParams(this, operation.Url + requestUriBuilder.ToString(), "GET", operation.Credentials, Conventions))
                            .AddOperationHeaders(OperationsHeaders);

            try
            {
                var response = (RavenJArray)await request.ReadResponseJsonAsync();
                return response.JsonDeserialization<FileHeader>();
            }
            catch ( Exception e )
            {
                throw e.SimplifyException();
            }
        }

        private async Task<RavenJObject> GetMetadataForAsyncImpl(string filename, OperationMetadata operation)
        {
            var request = RequestFactory.CreateHttpJsonRequest(
                                            new CreateHttpJsonRequestParams(this, operation.Url + "/files?name=" + Uri.EscapeDataString(filename), "HEAD", operation.Credentials, Conventions))
                                        .AddOperationHeaders(OperationsHeaders);

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

        public Task<Stream> DownloadAsync(string filename, Reference<RavenJObject> metadataRef = null, long? from = null, long? to = null)
        {
            return ExecuteWithReplication("GET", operation => DownloadAsyncImpl("/files/", filename, metadataRef, from, to, operation));
        }

        private async Task<Stream> DownloadAsyncImpl(string path, string filename, Reference<RavenJObject> metadataRef, long? @from, long? to, OperationMetadata operation)
        {
            var request = RequestFactory.CreateHttpJsonRequest(
                                            new CreateHttpJsonRequestParams(this, operation.Url + path + filename, "GET", operation.Credentials, Conventions))
                                        .AddOperationHeaders(OperationsHeaders);

            if (@from != null)
            {
                if (to != null)
                    request.AddRange(@from.Value, to.Value);
                else
                    request.AddRange(@from.Value);
            }

            try
            {
                var response = await request.ExecuteRawResponseAsync();
                if (response.StatusCode == HttpStatusCode.NotFound)
                    throw new FileNotFoundException("The file requested does not exists on the file system.", operation.Url + path + filename);

                await response.AssertNotFailingResponse();

                if ( metadataRef != null )
                {
                    var metadata = new RavenJObject();
                    foreach (var header in response.Headers)
                    {
                        var item = header.Value.SingleOrDefault();
                        if (item != null)
                        {
                            metadata[header.Key] = item;
                        }
                        else
                        {
                            metadata[header.Key] = RavenJObject.FromObject(header.Value);
                        }
                    }
                    foreach (var header in response.Content.Headers)
                    {
                        var item = header.Value.SingleOrDefault();
                        if (item != null)
                        {
                            metadata[header.Key] = item;
                        }
                        else
                        {
                            metadata[header.Key] = RavenJObject.FromObject(header.Value);
                        }
                    }

                    metadataRef.Value = metadata;
                }

                return await response.GetResponseStreamWithHttpDecompression();
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
                                                new CreateHttpJsonRequestParams(this, operation.Url + "/files/" + filename,"POST", operation.Credentials, Conventions))
                                            .AddOperationHeaders(OperationsHeaders);

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


        public Task UploadAsync(string filename, Stream source, long? size = null, Action<string, long> progress = null)
        {
            return UploadAsync(filename, source, null, size, progress);
        }

        public Task UploadAsync(string filename, Stream source, RavenJObject metadata, long? size = null, Action<string, long> progress = null)
        {
            if (metadata == null)
                metadata = new RavenJObject();

            return ExecuteWithReplication("PUT", async operation =>
            {
                if (source.CanRead == false)
                    throw new Exception("Stream does not support reading");

                var uploadIdentifier = Guid.NewGuid();
                var request = RequestFactory.CreateHttpJsonRequest(
                                new CreateHttpJsonRequestParams(this, operation.Url + "/files?name=" + Uri.EscapeDataString(filename) + "&uploadId=" + uploadIdentifier,
                                                                "PUT", operation.Credentials, Conventions))
                                            .AddOperationHeaders(OperationsHeaders);

                metadata["RavenFS-Size"] = size.HasValue ? new RavenJValue(size.Value) : new RavenJValue(source.Length);
                
                AddHeaders(metadata, request);

                var cts = new CancellationTokenSource();

                RegisterUploadOperation(uploadIdentifier, cts);

                try
                {
                    await request.WriteAsync(source);
                    if (request.ResponseStatusCode == HttpStatusCode.BadRequest)
                        throw new BadRequestException("There is a mismatch between the size reported in the RavenFS-Size header and the data read server side.");
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

        public IAsyncFilesSynchronizationCommands Synchronization
        {
            get
            {
                return new SynchronizationClient(this, Conventions);
            }
        }

        public IAsyncFilesConfigurationCommands Configuration
        {
            get { return new ConfigurationClient(this, Conventions); }
        }

        public IAsyncFilesStorageCommands Storage
        {
            get
            {
                return new StorageClient(this, Conventions);
            }
        }

        public IAsyncFilesAdminCommands Admin
        {
            get
            {
                return new AdminClient(this, Conventions);
            }
        }

        private static void AddHeaders(RavenJObject metadata, HttpJsonRequest request)
        {
            foreach( var item in metadata )
            {
	            var value = item.Value is RavenJValue ? item.Value.ToString() : item.Value.ToString(Formatting.None);
				request.AddHeader(item.Key, value); 
            }
        }

        public Task<string[]> GetDirectoriesAsync(string from = null, int start = 0, int pageSize = 25)
        {
            return ExecuteWithReplication("GET", async operation =>
            {
                var path = @from ?? "";
                if (path.StartsWith("/"))
                    path = path.Substring(1);

                var requestUriString = operation.Url + "/folders/subdirectories/" + Uri.EscapeUriString(path) + "?pageSize=" +
                                       pageSize + "&start=" + start;

                var request = RequestFactory.CreateHttpJsonRequest(
                                                new CreateHttpJsonRequestParams(this, requestUriString, "GET", operation.Credentials, Conventions))
                                            .AddOperationHeaders(OperationsHeaders);

                try
                {
                    var response = (RavenJArray) await request.ReadResponseJsonAsync();
                    return response.JsonDeserialization<string>();
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
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
            return ExecuteWithReplication("GET", async operation =>
            {
                var requestUriString = operation.Url + "/static/id";

                var request = RequestFactory.CreateHttpJsonRequest(
                                                new CreateHttpJsonRequestParams(this, requestUriString, "GET", operation.Credentials, Conventions))
                                            .AddOperationHeaders(OperationsHeaders);

                try
                {
                    var response = (RavenJValue) await request.ReadResponseJsonAsync();
                    return response.Value<Guid>();
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

        public class ConfigurationClient : IAsyncFilesConfigurationCommands, IHoldProfilingInformation
        {
            private readonly AsyncFilesServerClient client;
            private readonly FilesConvention convention;
            private readonly JsonSerializer jsonSerializer;

            public IAsyncFilesCommands Commands
            {
                get { return this.client; }
            }

            public ConfigurationClient(AsyncFilesServerClient client, FilesConvention convention)
            {
                this.jsonSerializer = new JsonSerializer();
                this.client = client;
                this.convention = convention;
            }

            public Task<string[]> GetKeyNamesAsync(int start = 0, int pageSize = 25)
            {
                return client.ExecuteWithReplication("GET", async operation =>
                {
                    var requestUriString = operation.Url + "/config?start=" + start + "&pageSize=" + pageSize;

                    var request = client.RequestFactory.CreateHttpJsonRequest(
                                                            new CreateHttpJsonRequestParams(this, requestUriString, "GET", operation.Credentials, convention))
                                                       .AddOperationHeaders(client.OperationsHeaders);

                    try
                    {
                        var response = (RavenJArray) await request.ReadResponseJsonAsync();
                        return response.JsonDeserialization<string>();
                    }
                    catch (Exception e)
                    {
                        throw e.SimplifyException();
                    }
                });
            }

            public Task SetKeyAsync<T>(string name, T data)
            {
                return client.ExecuteWithReplication("PUT", async operation =>
                {
                    var requestUriString = operation.Url + "/config?name=" + Uri.EscapeDataString(name);
                    var request = client.RequestFactory.CreateHttpJsonRequest(
                                            new CreateHttpJsonRequestParams(this, requestUriString, "PUT", operation.Credentials, convention))
                                        .AddOperationHeaders(client.OperationsHeaders);

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
                        await request.WriteWithObjectAsync(data);
                    }
                });
            }

            public Task DeleteKeyAsync(string name)
            {
                return client.ExecuteWithReplication("DELETE", operation =>
                {
                    var requestUriString = operation.Url + "/config?name=" + Uri.EscapeDataString(name);

                    var request = client.RequestFactory.CreateHttpJsonRequest(
                                            new CreateHttpJsonRequestParams(this, requestUriString, "DELETE", operation.Credentials, convention))
                                        .AddOperationHeaders(client.OperationsHeaders);

                    return request.ExecuteRequestAsync();
                });
            }

            public Task<T> GetKeyAsync<T>(string name)
            {
                return client.ExecuteWithReplication("GET", async operation =>
                {
                    var requestUriString = operation.Url + "/config?name=" + Uri.EscapeDataString(name);

                    var request = client.RequestFactory.CreateHttpJsonRequest(
                                            new CreateHttpJsonRequestParams(this, requestUriString, "GET", operation.Credentials, convention))
                                        .AddOperationHeaders(client.OperationsHeaders);

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

            public Task<ConfigurationSearchResults> SearchAsync(string prefix, int start = 0, int pageSize = 25)
            {
                return client.ExecuteWithReplication("GET", async operation =>
                {
                    var requestUriBuilder = new StringBuilder(operation.Url)
                        .Append("/config/search/?prefix=")
                        .Append(Uri.EscapeUriString(prefix))
                        .Append("&start=")
                        .Append(start)
                        .Append("&pageSize=")
                        .Append(pageSize);

                    var request = client.RequestFactory.CreateHttpJsonRequest(
                                            new CreateHttpJsonRequestParams(this, requestUriBuilder.ToString(), "GET", operation.Credentials, convention))
                                        .AddOperationHeaders(client.OperationsHeaders);

                    try
                    {
                        var response = (RavenJObject) await request.ReadResponseJsonAsync();
                        return response.JsonDeserialization<ConfigurationSearchResults>();
                    }
                    catch (Exception e)
                    {
                        throw e.SimplifyException();
                    }
                });
            }

            public void Dispose()
            {
            }

            public ProfilingInformation ProfilingInformation { get; private set; }
        }

        public class SynchronizationClient : IAsyncFilesSynchronizationCommands, IHoldProfilingInformation
        {
            private readonly OperationCredentials credentials;
            private readonly FilesConvention convention;
            private readonly AsyncFilesServerClient client;

            public IAsyncFilesCommands Commands
            {
                get { return this.client; }
            }

            public SynchronizationClient(AsyncFilesServerClient client, FilesConvention convention)
            {
                this.credentials = client.PrimaryCredentials;
                this.convention = convention;
                this.client = client;
            }

            public Task<RavenJObject> GetMetadataForAsync(string filename)
            {
                return client.GetMetadataForAsyncImpl(filename, new OperationMetadata(client.BaseUrl, credentials));
            }

            public async Task DownloadSignatureAsync(string sigName, Stream destination, long? from = null, long? to = null)
            {                
                var stream = await client.DownloadAsyncImpl("/rdc/signatures/", sigName, null, from, to, new OperationMetadata(client.BaseUrl, credentials));
                await stream.CopyToAsync(destination);
            }

            public async Task<SignatureManifest> GetRdcManifestAsync(string path)
            {
                var requestUriString = client.BaseUrl + "/rdc/manifest/" + Uri.EscapeDataString(path);
                var request = client.RequestFactory.CreateHttpJsonRequest(
                                                new CreateHttpJsonRequestParams(this, requestUriString, "GET", credentials, convention))
                                            .AddOperationHeaders(client.OperationsHeaders);

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

            public async Task<DestinationSyncResult[]> SynchronizeAsync(bool forceSyncingAll = false)
            {
                var requestUriString = String.Format("{0}/synchronization/ToDestinations?forceSyncingAll={1}", client.BaseUrl, forceSyncingAll);

                var request = client.RequestFactory.CreateHttpJsonRequest(
                                                    new CreateHttpJsonRequestParams(this, requestUriString, "POST", credentials, convention))
                                            .AddOperationHeaders(client.OperationsHeaders);

                try
                {
                    var response = (RavenJArray) await request.ReadResponseJsonAsync();
                    return response.JsonDeserialization<DestinationSyncResult>();                    
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            }

            public Task<SynchronizationReport> StartAsync(string fileName, IAsyncFilesCommands destination)
            {
                return StartAsync(fileName, destination.ToSynchronizationDestination());
            }

            public Task SetDestinationsAsync(params SynchronizationDestination[] destinations)
            {
                return client.ExecuteWithReplication("PUT", async operation =>
                {
                    var requestUriString = operation.Url + "/config?name=" + Uri.EscapeDataString(SynchronizationConstants.RavenSynchronizationDestinations);
                    var request = client.RequestFactory.CreateHttpJsonRequest(
                                            new CreateHttpJsonRequestParams(this, requestUriString, "PUT", operation.Credentials, convention))
                                        .AddOperationHeaders(client.OperationsHeaders);

                    var data = new { Destinations = destinations };

                    try
                    {
                        await request.WriteWithObjectAsync(data);
                    }
                    catch (Exception e)
                    {
                        throw e.SimplifyException();
                    }
                });
            }

            public async Task<SynchronizationReport> StartAsync(string fileName, SynchronizationDestination destination)
            {
                var requestUriString = String.Format("{0}/synchronization/start/{1}", client.BaseUrl, Uri.EscapeDataString(fileName));

                var request = client.RequestFactory.CreateHttpJsonRequest(
                                             new CreateHttpJsonRequestParams(this, requestUriString, "POST", credentials, convention))
                                   .AddOperationHeaders(client.OperationsHeaders);

                try
                {
                    await request.WriteWithObjectAsync(destination);
                    
                    var response = (RavenJObject)await request.ReadResponseJsonAsync();
                    return response.JsonDeserialization<SynchronizationReport>();
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            }

            public async Task<SynchronizationReport> GetSynchronizationStatusForAsync(string fileName)
            {
                var requestUriString = String.Format("{0}/synchronization/status/{1}", client.BaseUrl, Uri.EscapeDataString(fileName));

                var request = client.RequestFactory.CreateHttpJsonRequest(
                                                    new CreateHttpJsonRequestParams(this, requestUriString, "GET", credentials, convention))
                                    .AddOperationHeaders(client.OperationsHeaders);

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
                                                        client.BaseUrl, Uri.EscapeDataString(filename),
                                                        Uri.EscapeDataString(strategy.ToString()));

                var request = client.RequestFactory.CreateHttpJsonRequest(
                                    new CreateHttpJsonRequestParams(this, requestUriString,"PATCH", credentials, convention))
                                  .AddOperationHeaders(client.OperationsHeaders);

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
                                                   IEnumerable<HistoryItem> remoteHistory, string remoteServerUrl)
            {
                var requestUriString =
                    String.Format("{0}/synchronization/applyConflict/{1}?remoteVersion={2}&remoteServerId={3}&remoteServerUrl={4}",
                                  client.BaseUrl, Uri.EscapeDataString(filename), remoteVersion,
                                  Uri.EscapeDataString(remoteServerId), Uri.EscapeDataString(remoteServerUrl));

                var request = client.RequestFactory.CreateHttpJsonRequest(
                                    new CreateHttpJsonRequestParams(this, requestUriString, "PATCH", credentials, convention))
                                 .AddOperationHeaders(client.OperationsHeaders);

                try
                {
                    await request.WriteWithObjectAsync(remoteHistory);
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            }

            public async Task<ItemsPage<SynchronizationReport>> GetFinishedAsync(int page = 0, int pageSize = 25)
            {
                var requestUriString = String.Format("{0}/synchronization/finished?start={1}&pageSize={2}", client.BaseUrl, page,
                                                         pageSize);

                var request = client.RequestFactory.CreateHttpJsonRequest(
                                        new CreateHttpJsonRequestParams(this, requestUriString, "GET", credentials, convention))
                                   .AddOperationHeaders(client.OperationsHeaders);

                try
                {
                    var response = (RavenJObject) await request.ReadResponseJsonAsync();
                    return response.JsonDeserialization<ItemsPage<SynchronizationReport>>();
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            }

            public async Task<ItemsPage<SynchronizationDetails>> GetActiveAsync(int page = 0, int pageSize = 25)
            {
                var requestUriString = String.Format("{0}/synchronization/active?start={1}&pageSize={2}",
                                                        client.BaseUrl, page, pageSize);

                var request = client.RequestFactory.CreateHttpJsonRequest(
                                    new CreateHttpJsonRequestParams(this, requestUriString,"GET", credentials, convention))
                                 .AddOperationHeaders(client.OperationsHeaders);

                try
                {
                    var response = (RavenJObject)await request.ReadResponseJsonAsync();
                    return response.JsonDeserialization<ItemsPage<SynchronizationDetails>>();
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            }

            public async Task<ItemsPage<SynchronizationDetails>> GetPendingAsync(int page = 0, int pageSize = 25)
            {
                var requestUriString = String.Format("{0}/synchronization/pending?start={1}&pageSize={2}",
                                                     client.BaseUrl, page, pageSize);

                var request = client.RequestFactory.CreateHttpJsonRequest(
                                    new CreateHttpJsonRequestParams(this, requestUriString, "GET", credentials, convention))
                                 .AddOperationHeaders(client.OperationsHeaders);

                try
                {
                    var response = (RavenJObject) await request.ReadResponseJsonAsync();
                    return response.JsonDeserialization<ItemsPage<SynchronizationDetails>>();
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            }

            public async Task<SourceSynchronizationInformation> GetLastSynchronizationFromAsync(Guid serverId)
            {
                var requestUriString = String.Format("{0}/synchronization/LastSynchronization?from={1}", client.BaseUrl, serverId);

                var request = client.RequestFactory.CreateHttpJsonRequest(
                                    new CreateHttpJsonRequestParams(this, requestUriString, "GET", credentials, convention))
                                .AddOperationHeaders(client.OperationsHeaders);

                try
                {
                    var response = (RavenJObject) await request.ReadResponseJsonAsync();
                    return response.JsonDeserialization<SourceSynchronizationInformation>();
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            }

            public async Task<SynchronizationConfirmation[]> GetConfirmationForFilesAsync(IEnumerable<Tuple<string, Guid>> sentFiles)
            {
                var requestUriString = String.Format("{0}/synchronization/Confirm", client.BaseUrl);

                var request = client.RequestFactory.CreateHttpJsonRequest(
                                    new CreateHttpJsonRequestParams(this, requestUriString, "POST", credentials, convention))
                                .AddOperationHeaders(client.OperationsHeaders);

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

                        var response = (RavenJArray)await request.ReadResponseJsonAsync();
                        return response.JsonDeserialization<SynchronizationConfirmation>();
                    }

                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            }

            public async Task<ItemsPage<ConflictItem>> GetConflictsAsync(int page = 0, int pageSize = 25)
            {
                var requestUriString = String.Format("{0}/synchronization/conflicts?start={1}&pageSize={2}", client.BaseUrl, page,
                                                         pageSize);

                var request = client.RequestFactory.CreateHttpJsonRequest(
                                    new CreateHttpJsonRequestParams(this, requestUriString, "GET", credentials, convention))
                                 .AddOperationHeaders(client.OperationsHeaders);

                try
                {
                    var response = (RavenJObject)await request.ReadResponseJsonAsync();
                    return response.JsonDeserialization<ItemsPage<ConflictItem>>();
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
                                    client.BaseUrl, sourceServerId, sourceFileSystemUrl, sourceFileETag);

                var request = client.RequestFactory.CreateHttpJsonRequest(
                                    new CreateHttpJsonRequestParams(this, requestUriString, "POST", credentials, convention))
                                .AddOperationHeaders(client.OperationsHeaders);

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
                var requestUriString = client.BaseUrl + "/rdc/stats";

                var request = client.RequestFactory.CreateHttpJsonRequest(
                                    new CreateHttpJsonRequestParams(this, requestUriString, "GET", credentials, convention))
                                .AddOperationHeaders(client.OperationsHeaders);

                try
                {
                    var response = (RavenJObject)await request.ReadResponseJsonAsync();
                    return response.JsonDeserialization<RdcStats>();
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            }

            public async Task<SynchronizationReport> RenameAsync(string currentName, string newName, RavenJObject currentMetadata, ServerInfo sourceServer)
            {
                var request = client.RequestFactory.CreateHttpJsonRequest(
                                        new CreateHttpJsonRequestParams(this, client.BaseUrl + "/synchronization/rename?filename=" + Uri.EscapeDataString(currentName) + "&rename=" +
                                                                        Uri.EscapeDataString(newName), "PATCH", credentials, convention))
                                .AddOperationHeaders(client.OperationsHeaders);

                request.AddHeaders(currentMetadata);
                request.AddHeader(SyncingMultipartConstants.SourceServerInfo, sourceServer.AsJson());

                try
                {
                    var response = (RavenJObject)await request.ReadResponseJsonAsync();
                    return response.JsonDeserialization<SynchronizationReport>();
                }
                catch (ErrorResponseException exception)
                {
                    throw exception.SimplifyException();
                }
            }

            public async Task<SynchronizationReport> DeleteAsync(string fileName, RavenJObject metadata, ServerInfo sourceServer)
            {
                var request = client.RequestFactory.CreateHttpJsonRequest(
                                    new CreateHttpJsonRequestParams(this, client.BaseUrl + "/synchronization?fileName=" + Uri.EscapeDataString(fileName),
                                                                    "DELETE", credentials, convention))
                                .AddOperationHeaders(client.OperationsHeaders);

                request.AddHeaders(metadata);
                request.AddHeader(SyncingMultipartConstants.SourceServerInfo, sourceServer.AsJson());

                try
                {
                    var response = (RavenJObject) await request.ReadResponseJsonAsync();
                    return response.JsonDeserialization<SynchronizationReport>();
                }
                catch (ErrorResponseException exception)
                {
                    throw exception.SimplifyException();
                }
            }

            public async Task<SynchronizationReport> UpdateMetadataAsync(string fileName, RavenJObject metadata, ServerInfo sourceServer)
            {
                // REVIEW: (Oren) The ETag is always rewritten by this method as If-None-Match. Maybe a convention from the Database, but found it quite difficult to debug.  
                var request = client.RequestFactory.CreateHttpJsonRequest(
                                    new CreateHttpJsonRequestParams(this, client.BaseUrl + "/synchronization/UpdateMetadata/" + Uri.EscapeDataString(fileName), "POST", credentials, convention))
                                 .AddOperationHeaders(client.OperationsHeaders);

                request.AddHeaders(metadata);
                request.AddHeader(SyncingMultipartConstants.SourceServerInfo, sourceServer.AsJson());
                // REVIEW: (Oren) and also causes this.
                request.AddHeader("ETag", "\"" + metadata.Value<string>("ETag") + "\"");

                try
                {
                    var response = (RavenJObject) await request.ReadResponseJsonAsync();
                    return response.JsonDeserialization<SynchronizationReport>();
                }
                catch (ErrorResponseException exception)
                {
                    throw exception.SimplifyException();
                }
            }
            public void Dispose()
            {
            }

            public ProfilingInformation ProfilingInformation { get; private set; }
        }

        public class StorageClient : IAsyncFilesStorageCommands, IHoldProfilingInformation
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
                return client.ExecuteWithReplication("POST", async operation =>
                {
                    var requestUriString = String.Format("{0}/storage/cleanup", operation.Url);

                    var request = client.RequestFactory.CreateHttpJsonRequest(
                                            new CreateHttpJsonRequestParams(this, requestUriString,"POST", operation.Credentials, convention))
                                    .AddOperationHeaders(client.OperationsHeaders);

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

            public Task RetryRenamingAsync()
            {
                return client.ExecuteWithReplication("POST", async operation =>
                {
                    var requestUriString = String.Format("{0}/storage/retryrenaming", operation.Url);

                    var request = client.RequestFactory.CreateHttpJsonRequest(
                                        new CreateHttpJsonRequestParams(this, requestUriString, "POST", operation.Credentials, convention))
                                     .AddOperationHeaders(client.OperationsHeaders);

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

            public void Dispose()
            {
            }
        }

        public class AdminClient : IAsyncFilesAdminCommands, IHoldProfilingInformation
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
                var requestUriString = string.Format("{0}/fs/names", client.ServerUrl);

                var request =
                    client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString,
                        "GET", client.PrimaryCredentials, convention));

                try
                {
                    var response = (RavenJArray)await request.ReadResponseJsonAsync();
                    return response.JsonDeserialization<string>();
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            }

            public async Task<FileSystemStats[]> GetStatisticsAsync()
            {
                var requestUriString = string.Format("{0}/fs/stats", client.ServerUrl);

                var request =
                    client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString,
                        "GET", client.PrimaryCredentials, convention));

                try
                {
                    var response = (RavenJArray)await request.ReadResponseJsonAsync();
                    return response.JsonDeserialization<FileSystemStats>();
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            }

            public async Task CreateFileSystemAsync(FileSystemDocument filesystemDocument, string newFileSystemName = null)
            {
                var requestUriString = string.Format("{0}/fs/admin/{1}", client.ServerUrl,
                                                     newFileSystemName ?? client.FileSystem);

                var request = client.RequestFactory.CreateHttpJsonRequest(
                                        new CreateHttpJsonRequestParams(this, requestUriString,
                                                                        "PUT", client.PrimaryCredentials, convention));

                try
                {
                    await request.WriteWithObjectAsync(filesystemDocument);
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

            public async Task CreateOrUpdateFileSystemAsync(FileSystemDocument filesystemDocument, string newFileSystemName = null)
            {
                var requestUriString = string.Format("{0}/fs/admin/{1}?update=true", client.ServerUrl,
                                                     newFileSystemName ?? client.FileSystem);

                var request = client.RequestFactory.CreateHttpJsonRequest(
                                        new CreateHttpJsonRequestParams(this, requestUriString,
                                                                        "PUT", client.PrimaryCredentials, convention));

                try
                {
                    await request.WriteWithObjectAsync(filesystemDocument);                    
                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            }

			public async Task DeleteFileSystemAsync(string fileSystemName = null, bool hardDelete = false)
			{
                var requestUriString = string.Format("{0}/fs/admin/{1}?hard-delete={2}", client.ServerUrl, fileSystemName ?? client.FileSystem, hardDelete);

				var request = client.RequestFactory.CreateHttpJsonRequest(
										new CreateHttpJsonRequestParams(this, requestUriString,
																		"DELETE", client.PrimaryCredentials, convention));

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

            public void Dispose()
            {
            }
        }

        public override void Dispose()
        {            
            if (notifications.IsValueCreated)
                notifications.Value.Dispose();

            base.Dispose();
        }

        public ProfilingInformation ProfilingInformation { get; private set; }
    }
}