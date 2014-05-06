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
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.OAuth;
using Raven.Abstractions.RavenFS;
using Raven.Client.Connection;
using Raven.Client.Connection.Profiling;
using Raven.Client.RavenFS.Changes;
using Raven.Client.RavenFS.Connections;
using Raven.Client.RavenFS.Extensions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Client.RavenFS
{
    public class RavenFileSystemClient : IDisposable, IHoldProfilingInformation
    {
        private readonly ServerNotifications notifications;
        private OperationCredentials credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication;
        private IDisposable failedUploadsObserver;
        private readonly IFileSystemClientReplicationInformer replicationInformer;
        private readonly FileConvention convention;
        private int readStripingBase;
        private HttpJsonRequestFactory jsonRequestFactory =
#if !NETFX_CORE
              new HttpJsonRequestFactory(DefaultNumberOfCachedRequests);
#else
			  new HttpJsonRequestFactory();
#endif

        private const int DefaultNumberOfCachedRequests = 2048;

        private readonly ConcurrentDictionary<Guid, CancellationTokenSource> uploadCancellationTokens =
            new ConcurrentDictionary<Guid, CancellationTokenSource>();

        /// <summary>
        /// Notify when the failover status changed
        /// </summary>
        public event EventHandler<FailoverStatusChangedEventArgs> FailoverStatusChanged
        {
            add { replicationInformer.FailoverStatusChanged += value; }
            remove { replicationInformer.FailoverStatusChanged -= value; }
        }

        /// <summary>
        /// Allow access to the replication informer used to determine how we replicate requests
        /// </summary>
        public IFileSystemClientReplicationInformer ReplicationInformer
        {
            get { return replicationInformer; }
        }

        public OperationCredentials PrimaryCredentials
        {
            get { return credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication; }
        }

        public RavenFileSystemClient(string serverUrl, string fileSystemName, ICredentials credentials = null, string apiKey = null)
        {
            try
            {
                ServerUrl = serverUrl;
                if (ServerUrl.EndsWith("/"))
                    ServerUrl = ServerUrl.Substring(0, ServerUrl.Length - 1);

                FileSystemName = fileSystemName;
                Credentials = credentials ?? CredentialCache.DefaultNetworkCredentials;
                ApiKey = apiKey;

                convention = new FileConvention();
                notifications = new ServerNotifications(serverUrl, convention);
                replicationInformer = new RavenFileSystemReplicationInformer(convention);
                readStripingBase = replicationInformer.GetReadStripingBase();

                InitializeSecurity();
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }

        }

        public string ServerUrl { get; private set; }

        public string FileSystemName { get; private set; }

        public string FileSystemUrl
        {
            get { return string.Format("{0}/ravenfs/{1}", ServerUrl, FileSystemName); }
        }

        public ICredentials Credentials { get; private set; }

        public string ApiKey { get; private set; }

        public bool IsObservingFailedUploads
        {
            get { return failedUploadsObserver != null; }
            set
            {
                if (value)
                {
                    failedUploadsObserver = notifications.FailedUploads().Subscribe(CancelFileUpload);
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
            if (convention.HandleUnauthorizedResponseAsync != null)
                return; // already setup by the user

            if (string.IsNullOrEmpty(ApiKey) == false)
            {
                Credentials = null;
            }

            credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication = new OperationCredentials(ApiKey, Credentials);

            var basicAuthenticator = new BasicAuthenticator(jsonRequestFactory.EnableBasicAuthenticationOverUnsecuredHttpEvenThoughPasswordsWouldBeSentOverTheWireInClearTextToBeStolenByHackers);
            var securedAuthenticator = new SecuredAuthenticator();

            jsonRequestFactory.ConfigureRequest += basicAuthenticator.ConfigureRequest;
            jsonRequestFactory.ConfigureRequest += securedAuthenticator.ConfigureRequest;

            convention.HandleForbiddenResponseAsync = (forbiddenResponse, credentials) =>
            {
                if (credentials.ApiKey == null)
                {
                    AssertForbiddenCredentialSupportWindowsAuth(forbiddenResponse);
                    return null;
                }

                return null;
            };

            convention.HandleUnauthorizedResponseAsync = (unauthorizedResponse, credentials) =>
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
            if (Credentials == null)
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
                    jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString.NoCache(),
                        "GET", operation.Credentials, convention));

                try
                {
                    var response = (RavenJObject) await request.ReadResponseJsonAsync();
                    return JsonExtensions.JsonDeserialization<FileSystemStats>(response);
                }
                catch (Exception e)
                {
                    throw e.TryThrowBetterError();
                }
            });
        }

        public Task DeleteAsync(string filename)
        {
            return ExecuteWithReplication("DELETE", async operation =>
            {
                var requestUriString = operation.Url + "/files/" + Uri.EscapeDataString(filename);

                var request = jsonRequestFactory.CreateHttpJsonRequest(
                                        new CreateHttpJsonRequestParams(this, requestUriString,
                                                                        "DELETE", operation.Credentials, convention));

                try
                {
                    await request.ExecuteRequestAsync();
                }
                catch (Exception e)
                {
                    throw e.TryThrowBetterError();
                }
            });
        }

        public Task RenameAsync(string filename, string rename)
        {
            return ExecuteWithReplication("PATCH", async operation =>
            {
                var requestUriString = operation.Url + "/files/" + Uri.EscapeDataString(filename) + "?rename=" +
                                       Uri.EscapeDataString(rename);

                var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, 
                                                                                                       "PATCH", operation.Credentials, convention));

                try
                {
                    await request.ExecuteRequestAsync();
                }
                catch (Exception e)
                {
                    throw e.TryThrowBetterError();
                }
            });
        }

        public Task<FileInfo[]> BrowseAsync(int start = 0, int pageSize = 25)
        {
            return ExecuteWithReplication("GET", async operation =>
            {
                var request = jsonRequestFactory.CreateHttpJsonRequest(
                                    new CreateHttpJsonRequestParams(this, (operation.Url + "/files?start=" + start + "&pageSize=" + pageSize).NoCache(),
                                                                    "GET", operation.Credentials, convention));

                try
                {
                    var response = (RavenJArray) await request.ReadResponseJsonAsync();
                    var items = response.Select(x => JsonExtensions.JsonDeserialization<FileInfo>((RavenJObject)x));
                    return items.ToArray();
                }
                catch (Exception e)
                {
                    throw e.TryThrowBetterError();
                }
            });
        }

        private int requestCount;
        private volatile bool currentlyExecuting;

        private Task<T> ExecuteWithReplication<T>(string method, Func<OperationMetadata, Task<T>> operation)
        {
            var currentRequest = Interlocked.Increment(ref requestCount);
            if (currentlyExecuting && convention.AllowMultipuleAsyncOperations == false)
                throw new InvalidOperationException("Only a single concurrent async request is allowed per async client instance.");

            currentlyExecuting = true;
            try
            {
                return replicationInformer.ExecuteWithReplicationAsync(method, FileSystemUrl,
                    credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication, currentRequest, readStripingBase, operation)
                    .ContinueWith(task =>
                    {
                        currentlyExecuting = false;
                        return task;
                    }).Unwrap();
            }
            catch (Exception)
            {
                currentlyExecuting = false;
                throw;
            }
        }

        private Task ExecuteWithReplication(string method, Func<OperationMetadata, Task> operation)
        {
            // Convert the Func<string, Task> to a Func<string, Task<object>>
            return ExecuteWithReplication(method, async u =>
            {
                await operation(u);
                return (object)null;
            });
        }

        public Task<string[]> GetSearchFieldsAsync(int start = 0, int pageSize = 25)
        {
            return ExecuteWithReplication("GET", async operation =>
            {
                var requestUriString = string.Format("{0}/search/terms?start={1}&pageSize={2}", operation.Url, start, pageSize).NoCache();
                var request = jsonRequestFactory.CreateHttpJsonRequest(
                                        new CreateHttpJsonRequestParams(this, requestUriString,
                                                                        "GET", operation.Credentials, convention));

                try
                {
                    var response = (RavenJArray) await request.ReadResponseJsonAsync();
                    var items = response.Select(x => x.Value<string>());
                    return items.ToArray();
                }
                catch (Exception e)
                {
                    throw e.TryThrowBetterError();
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

                var request = jsonRequestFactory.CreateHttpJsonRequest(
                                                        new CreateHttpJsonRequestParams(this, requestUriBuilder.ToString().NoCache(),
                                                                                        "GET", operation.Credentials, convention));
                try
                {
                    var response = (RavenJObject)await request.ReadResponseJsonAsync();        
                    
                    return JsonExtensions.JsonDeserialization<SearchResults>(response);
                }
                catch (Exception e)
                {
                    throw e.TryThrowBetterError();
                }
            });

        }

        public Task<RavenJObject> GetMetadataForAsync(string filename)
        {
            return ExecuteWithReplication("HEAD", operation => GetMetadataForAsyncImpl(filename, operation));
        }

        private async Task<RavenJObject> GetMetadataForAsyncImpl(string filename, OperationMetadata operation)
        {
            var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operation.Url + "/files?name=" + Uri.EscapeDataString(filename),
                                                                                                   "HEAD", operation.Credentials, convention));

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
                    throw e.TryThrowBetterError();
                }

                throw e.TryThrowBetterError();
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
                jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operation.Url + path + filename,
                                                                                         "GET", operation.Credentials, convention));

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
                throw e.TryThrowBetterError();
            }
        }


        public Task UpdateMetadataAsync(string filename, RavenJObject metadata)
        {
            return ExecuteWithReplication("POST", async operation =>
            {
                var request = jsonRequestFactory.CreateHttpJsonRequest(
                                    new CreateHttpJsonRequestParams(this, operation.Url + "/files/" + filename,
                                                                    "POST", operation.Credentials, convention));

                AddHeaders(metadata, request);

                try
                {
                    await request.ExecuteRequestAsync();
                }
                catch (Exception e)
                {
                    throw e.TryThrowBetterError();
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
                var request = jsonRequestFactory.CreateHttpJsonRequest(
                                new CreateHttpJsonRequestParams(this, operation.Url + "/files?name=" + Uri.EscapeDataString(filename) + "&uploadId=" + uploadIdentifier, 
                                                                "PUT", operation.Credentials, convention));

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
                    throw e.TryThrowBetterError();
                }
                finally
                {
                    UnregisterUploadOperation(uploadIdentifier);
                }
            });
        }

        private void CancelFileUpload(UploadFailed uploadFailed)
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

        public IServerNotifications Notifications
        {
            get { return notifications; }
        }

        public FileConvention Convention
        {
            get { return convention; }
        }

        private static void AddHeaders(RavenJObject metadata, HttpJsonRequest request)
        {
            foreach( var item in metadata )
            {
                request.AddHeader(item.Key, item.Value.ToString());
            }
        }

        private static void AddHeaders(NameValueCollection metadata, HttpJsonRequest request)
        {
            foreach (var key in metadata.AllKeys)
            {
                var values = metadata.GetValues(key);
                if (values == null)
                    continue;
                foreach (var value in values)
                {
                    request.AddHeader(key, value);
                }
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
                    jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString.NoCache(),
                        "GET", operation.Credentials, convention));

                try
                {
                    var response = await request.ReadResponseJsonAsync();

                    return new JsonSerializer().Deserialize<string[]>(new RavenJTokenReader(response));
                }
                catch (Exception e)
                {
                    throw e.TryThrowBetterError();
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
                var requestUriString = operation.Url + "/staticfs/id/";

                var request = jsonRequestFactory.CreateHttpJsonRequest(
                                        new CreateHttpJsonRequestParams(this, requestUriString, "GET", operation.Credentials, convention));

                try
                {
                    var response = await request.ReadResponseJsonAsync();
                    return new JsonSerializer().Deserialize<Guid>(new RavenJTokenReader(response));
                }
                catch (Exception e)
                {
                    throw e.TryThrowBetterError();
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
            private readonly RavenFileSystemClient ravenFileSystemClient;
            private readonly FileConvention convention;
            private readonly JsonSerializer jsonSerializer;

            public ConfigurationClient(RavenFileSystemClient ravenFileSystemClient, FileConvention convention)
            {
                jsonSerializer = new JsonSerializer
                {
                    Converters =
						{
							new NameValueCollectionJsonConverter()
						}
                };

                this.ravenFileSystemClient = ravenFileSystemClient;
                this.convention = convention;
            }

            public Task<string[]> GetConfigNames(int start = 0, int pageSize = 25)
            {
                return ravenFileSystemClient.ExecuteWithReplication("GET", async operation =>
                {
                    var requestUriString = operation.Url + "/config?start=" + start + "&pageSize=" + pageSize;

                    var request =
                        ravenFileSystemClient.jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString.NoCache(),
                            "GET", operation.Credentials, convention));

                    try
                    {
                        var response = await request.ReadResponseJsonAsync();
                        return jsonSerializer.Deserialize<string[]>(new RavenJTokenReader(response));
                    }
                    catch (Exception e)
                    {
                        throw e.TryThrowBetterError();
                    }
                });
            }

            public Task SetConfig<T>(string name, T data)
            {
                return ravenFileSystemClient.ExecuteWithReplication("PUT", async operation =>
                {
                    var requestUriString = operation.Url + "/config?name=" + StringUtils.UrlEncode(name);
                    var request = ravenFileSystemClient.jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "PUT", operation.Credentials, convention));

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
                    var requestUriString = operation.Url + "/config?name=" + StringUtils.UrlEncode(SynchronizationConstants.RavenSynchronizationDestinations);
                    var request = ravenFileSystemClient.jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "PUT", operation.Credentials, convention));

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
                    var requestUriString = operation.Url + "/config?name=" + StringUtils.UrlEncode(name);

                    var request = ravenFileSystemClient.jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "DELETE", operation.Credentials, convention));

                    return request.ExecuteRequestAsync();
                });
            }

            public Task<T> GetConfig<T>(string name)
            {
                return ravenFileSystemClient.ExecuteWithReplication("GET", async operation =>
                {
                    var requestUriString = operation.Url + "/config?name=" + StringUtils.UrlEncode(name);

                    var request = ravenFileSystemClient.jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString.NoCache(), "GET", operation.Credentials, convention));

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
                    ravenFileSystemClient.jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriBuilder.ToString().NoCache(),
                        "GET", operation.Credentials, convention));

                    try
                    {
                        var response = await request.ReadResponseJsonAsync();
                        using (var jsonTextReader = new RavenJTokenReader(response))
                        {
                            return new JsonSerializer().Deserialize<ConfigSearchResults>(jsonTextReader);
                        }
                    }
                    catch (Exception e)
                    {
                        throw e.TryThrowBetterError();
                    }
                });
            }

            public ProfilingInformation ProfilingInformation { get; private set; }
        }

        public class SynchronizationClient : IHoldProfilingInformation
        {
            private readonly OperationCredentials credentials;
            private readonly FileConvention convention;
            private readonly HttpJsonRequestFactory jsonRequestFactory;
            private readonly RavenFileSystemClient fullClient;

            public SynchronizationClient(RavenFileSystemClient client, FileConvention convention)
            {
                credentials = client.PrimaryCredentials;
                jsonRequestFactory = client.jsonRequestFactory;
                this.convention = convention;
                FileSystemUrl = client.FileSystemUrl;
                fullClient = client;
            }

            public string FileSystemUrl { get; private set; }

            public FileConvention Convention
            {
                get { return convention; }
            }

            public OperationCredentials Credentials
            {
                get { return credentials; }
            }

            public HttpJsonRequestFactory JsonRequestFactory
            {
                get { return jsonRequestFactory; }
            }

            public Task<RavenJObject> GetMetadataForAsync(string filename)
            {
                return fullClient.GetMetadataForAsyncImpl(filename, new OperationMetadata(FileSystemUrl, credentials));
            }

            public Task DownloadSignatureAsync(string sigName, Stream destination, long? from = null, long? to = null)
            {
                return fullClient.DownloadAsyncImpl("/rdc/signatures/", sigName, destination, from, to, null, new OperationMetadata(FileSystemUrl, credentials));
            }

            public async Task<SignatureManifest> GetRdcManifestAsync(string path)
            {
                var requestUriString = FileSystemUrl + "/rdc/manifest/" + StringUtils.UrlEncode(path);
                var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString,
                                                                                                       "GET", credentials, convention));

                try
                {
                    var response = (RavenJObject) await request.ReadResponseJsonAsync();
                    return response.JsonDeserialization<SignatureManifest>();
                }
                catch (Exception e)
                {
                    throw e.TryThrowBetterError();
                }
            }

            public async Task<DestinationSyncResult[]> SynchronizeDestinationsAsync(bool forceSyncingAll = false)
            {
                var requestUriString = String.Format("{0}/synchronization/ToDestinations?forceSyncingAll={1}", FileSystemUrl, forceSyncingAll);

                var request = jsonRequestFactory.CreateHttpJsonRequest(
                                                    new CreateHttpJsonRequestParams(this, requestUriString.NoCache(),
                                                                                    "POST", credentials, convention));

                try
                {
                    var response = (RavenJArray) await request.ReadResponseJsonAsync();
                    return response.Select(x => ((RavenJObject)x).JsonDeserialization<DestinationSyncResult>()).ToArray();
                }
                catch (Exception e)
                {
                    throw e.TryThrowBetterError();
                }
            }

            public Task<SynchronizationReport> StartAsync(string fileName, RavenFileSystemClient destination)
            {
                return StartAsync(fileName, destination.ToSynchronizationDestination());
            }

            public async Task<SynchronizationReport> StartAsync(string fileName, SynchronizationDestination destination)
            {
                var requestUriString = String.Format("{0}/synchronization/start/{1}", FileSystemUrl, Uri.EscapeDataString(fileName));

                var request = jsonRequestFactory.CreateHttpJsonRequest(
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
                    throw e.TryThrowBetterError();
                }
            }

            public async Task<SynchronizationReport> GetSynchronizationStatusAsync(string fileName)
            {
                var requestUriString = String.Format("{0}/synchronization/status/{1}", FileSystemUrl, Uri.EscapeDataString(fileName));

                var request = jsonRequestFactory.CreateHttpJsonRequest(
                                                    new CreateHttpJsonRequestParams(this, requestUriString.NoCache(),
                                                                                    "GET", credentials, convention));

                try
                {
                    var response = (RavenJObject)await request.ReadResponseJsonAsync();
                    return response.JsonDeserialization<SynchronizationReport>();
                }
                catch (Exception e)
                {
                    throw e.TryThrowBetterError();
                }
            }

            public async Task ResolveConflictAsync(string filename, ConflictResolutionStrategy strategy)
            {
                var requestUriString = String.Format("{0}/synchronization/resolveConflict/{1}?strategy={2}",
                                                        FileSystemUrl, Uri.EscapeDataString(filename),
                                                        Uri.EscapeDataString(strategy.ToString()));

                var request = jsonRequestFactory.CreateHttpJsonRequest(
                                    new CreateHttpJsonRequestParams(this, requestUriString,
                                                                    "PATCH", credentials, convention));

                try
                {
                    await request.ExecuteRequestAsync();
                }
                catch (Exception e)
                {
                    throw e.TryThrowBetterError();
                }
            }

            public async Task ApplyConflictAsync(string filename, long remoteVersion, string remoteServerId,
                                                   IList<HistoryItem> remoteHistory, string remoteServerUrl)
            {
                var requestUriString =
                    String.Format("{0}/synchronization/applyConflict/{1}?remoteVersion={2}&remoteServerId={3}&remoteServerUrl={4}",
                                  FileSystemUrl, Uri.EscapeDataString(filename), remoteVersion,
                                  Uri.EscapeDataString(remoteServerId), Uri.EscapeDataString(remoteServerUrl));

                var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString,
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
                    throw e.TryThrowBetterError();
                }
            }

            public async Task<ListPage<SynchronizationReport>> GetFinishedAsync(int page = 0, int pageSize = 25)
            {
                var requestUriString = String.Format("{0}/synchronization/finished?start={1}&pageSize={2}", FileSystemUrl, page,
                                                         pageSize);

                var request = jsonRequestFactory.CreateHttpJsonRequest(
                                        new CreateHttpJsonRequestParams(this, requestUriString.NoCache(),
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
                    throw e.TryThrowBetterError();
                }
            }

            public async Task<ListPage<SynchronizationDetails>> GetActiveAsync(int page = 0, int pageSize = 25)
            {
                var requestUriString = String.Format("{0}/synchronization/active?start={1}&pageSize={2}",
                                                        FileSystemUrl, page, pageSize);

                var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString.NoCache(),
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
                    throw e.TryThrowBetterError();
                }
            }

            public async Task<ListPage<SynchronizationDetails>> GetPendingAsync(int page = 0, int pageSize = 25)
            {
                var requestUriString = String.Format("{0}/synchronization/pending?start={1}&pageSize={2}",
                                                     FileSystemUrl, page, pageSize);

                var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString.NoCache(),
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
                    throw e.TryThrowBetterError();
                }
            }

            public async Task<SourceSynchronizationInformation> GetLastSynchronizationFromAsync(Guid serverId)
            {
                var requestUriString = String.Format("{0}/synchronization/LastSynchronization?from={1}", FileSystemUrl, serverId);

                var request = jsonRequestFactory.CreateHttpJsonRequest(
                                                    new CreateHttpJsonRequestParams(this, requestUriString.NoCache(),
                                                                                    "GET", credentials, convention));

                try
                {
                    var response = await request.ReadResponseJsonAsync();
                    var preResult = new JsonSerializer().Deserialize<SourceSynchronizationInformation>(new RavenJTokenReader(response));
                    return preResult;
                }
                catch (Exception e)
                {
                    throw e.TryThrowBetterError();
                }
            }

            public async Task<IEnumerable<SynchronizationConfirmation>> ConfirmFilesAsync(IEnumerable<Tuple<string, Guid>> sentFiles)
            {
                var requestUriString = String.Format("{0}/synchronization/Confirm", FileSystemUrl);

                var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "POST", credentials, convention));

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
                    throw e.TryThrowBetterError();
                }
            }

            public async Task<ListPage<ConflictItem>> GetConflictsAsync(int page = 0, int pageSize = 25)
            {
                var requestUriString = String.Format("{0}/synchronization/conflicts?start={1}&pageSize={2}", FileSystemUrl, page,
                                                         pageSize);

                var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString.NoCache(),
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
                    throw e.TryThrowBetterError();
                }
            }

            public async Task IncrementLastETagAsync(Guid sourceServerId, string sourceFileSystemUrl, Guid sourceFileETag)
            {
                var requestUriString =
                    String.Format("{0}/synchronization/IncrementLastETag?sourceServerId={1}&sourceFileSystemUrl={2}&sourceFileETag={3}",
                                    FileSystemUrl, sourceServerId, sourceFileSystemUrl, sourceFileETag);

                var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString.NoCache(),
                        "POST", credentials, convention));

                try
                {
                    await request.ExecuteRequestAsync();
                }
                catch (Exception e)
                {
                    throw e.TryThrowBetterError();
                }
            }

            public async Task<RdcStats> GetRdcStatsAsync()
            {
                var requestUriString = FileSystemUrl + "/rdc/stats";

                var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString.NoCache(),
                        "GET", credentials, convention));

                try
                {
                    var response = await request.ReadResponseJsonAsync();
                    return new JsonSerializer().Deserialize<RdcStats>(new RavenJTokenReader(response));
                }
                catch (Exception e)
                {
                    throw e.TryThrowBetterError();
                }
            }

            public async Task<SynchronizationReport> RenameAsync(string currentName, string newName, RavenJObject currentMetadata, ServerInfo sourceServer)
            {
                var request = jsonRequestFactory.CreateHttpJsonRequest(
                                        new CreateHttpJsonRequestParams(this, FileSystemUrl + "/synchronization/rename?filename=" + Uri.EscapeDataString(currentName) + "&rename=" +
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
                    throw exception.BetterWebExceptionError();
                }
            }

            public async Task<SynchronizationReport> DeleteAsync(string fileName, RavenJObject metadata, ServerInfo sourceServer)
            {
                var request = jsonRequestFactory.CreateHttpJsonRequest(
                                    new CreateHttpJsonRequestParams(this, FileSystemUrl + "/synchronization?fileName=" + Uri.EscapeDataString(fileName),
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
                    throw exception.BetterWebExceptionError();
                }
            }

            public async Task<SynchronizationReport> UpdateMetadataAsync(string fileName, RavenJObject metadata, ServerInfo sourceServer)
            {
                // REVIEW: (Oren) The ETag is always rewritten by this method as If-None-Match. Maybe a convention from the Database, but found it quite difficult to debug.  
                var request = jsonRequestFactory.CreateHttpJsonRequest(
                                    new CreateHttpJsonRequestParams(this, FileSystemUrl + "/synchronization/UpdateMetadata/" + Uri.EscapeDataString(fileName),
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
                    throw exception.BetterWebExceptionError();
                }
            }

            public ProfilingInformation ProfilingInformation { get; private set; }
        }

        public class StorageClient : IHoldProfilingInformation
        {
            private readonly RavenFileSystemClient ravenFileSystemClient;
            private readonly FileConvention convention;


            public StorageClient(RavenFileSystemClient ravenFileSystemClient, FileConvention convention)
            {
                this.ravenFileSystemClient = ravenFileSystemClient;
                this.convention = convention;
            }

            public Task CleanUp()
            {
                return ravenFileSystemClient.ExecuteWithReplication("POST", async operation =>
                {
                    var requestUriString = String.Format("{0}/storage/cleanup", operation.Url);

                    var request = ravenFileSystemClient.jsonRequestFactory.CreateHttpJsonRequest(
                                            new CreateHttpJsonRequestParams(this, requestUriString.NoCache(),
                                                                            "POST", operation.Credentials, convention));

                    try
                    {
                        await request.ExecuteRequestAsync();
                    }
                    catch (Exception e)
                    {
                        throw e.TryThrowBetterError();
                    }
                });
            }

            public Task RetryRenaming()
            {
                return ravenFileSystemClient.ExecuteWithReplication("POST", async operation =>
                {
                    var requestUriString = String.Format("{0}/storage/retryrenaming", operation.Url);

                    var request = ravenFileSystemClient.jsonRequestFactory.CreateHttpJsonRequest(
                                        new CreateHttpJsonRequestParams(this, requestUriString.NoCache(),
                                                                        "POST", operation.Credentials, convention));

                    try
                    {
                        await request.ExecuteRequestAsync();
                    }
                    catch (Exception e)
                    {
                        throw e.TryThrowBetterError();
                    }
                });
            }

            public ProfilingInformation ProfilingInformation { get; private set; }
        }

        public class AdminClient : IHoldProfilingInformation
        {
            private readonly RavenFileSystemClient ravenFileSystemClient;
            private readonly FileConvention convention;

            public AdminClient(RavenFileSystemClient ravenFileSystemClient, FileConvention convention)
            {
                this.ravenFileSystemClient = ravenFileSystemClient;
                this.convention = convention;
            }

            public async Task<string[]> GetFileSystemsNames()
            {
                var requestUriString = string.Format("{0}/ravenfs/names", ravenFileSystemClient.ServerUrl);

                var request =
                    ravenFileSystemClient.jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString.NoCache(),
                        "GET", ravenFileSystemClient.PrimaryCredentials, convention));

                try
                {
                    var response = await request.ReadResponseJsonAsync();
                    return new JsonSerializer().Deserialize<string[]>(new RavenJTokenReader(response));
                }
                catch (Exception e)
                {
                    throw e.TryThrowBetterError();
                }
            }

            public async Task<List<FileSystemStats>> GetFileSystemsStats()
            {
                var requestUriString = string.Format("{0}/ravenfs/stats", ravenFileSystemClient.ServerUrl);

                var request =
                    ravenFileSystemClient.jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString.NoCache(),
                        "GET", ravenFileSystemClient.PrimaryCredentials, convention));

                try
                {
                    var response = await request.ReadResponseJsonAsync();
                    return new JsonSerializer().Deserialize<List<FileSystemStats>>(new RavenJTokenReader(response));
                }
                catch (Exception e)
                {
                    throw e.TryThrowBetterError();
                }
            }

            public Task CreateFileSystemAsync(DatabaseDocument databaseDocument, string newFileSystemName = null)
            {
                var requestUriString = string.Format("{0}/ravenfs/admin/{1}", ravenFileSystemClient.ServerUrl,
                                                     newFileSystemName ?? ravenFileSystemClient.FileSystemName);

                var request = ravenFileSystemClient.jsonRequestFactory.CreateHttpJsonRequest(
                                        new CreateHttpJsonRequestParams(this, requestUriString.NoCache(),
                                                                        "PUT", ravenFileSystemClient.PrimaryCredentials, convention));

                try
                {
                    return request.WriteAsync(JsonConvert.SerializeObject(databaseDocument));
                }
                catch (Exception e)
                {
                    throw e.TryThrowBetterError();
                }
            }

            public ProfilingInformation ProfilingInformation { get; private set; }
        }

        public void Dispose()
        {
            if (notifications != null)
                notifications.Dispose();
        }

        public ProfilingInformation ProfilingInformation { get; private set; }
    }
}