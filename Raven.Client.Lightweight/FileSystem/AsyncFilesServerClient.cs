using System.Diagnostics;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.FileSystem.Notifications;
using Raven.Abstractions.OAuth;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Connection.Profiling;
using Raven.Client.Extensions;
using Raven.Client.FileSystem.Changes;
using Raven.Client.FileSystem.Connection;
using Raven.Client.FileSystem.Extensions;
using Raven.Client.FileSystem.Listeners;
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
using FileSystemInfo = Raven.Abstractions.FileSystem.FileSystemInfo;

namespace Raven.Client.FileSystem
{

    public class AsyncFilesServerClient : AsyncServerClientBase<FilesConvention, IFilesReplicationInformer>, IAsyncFilesCommandsImpl
    {
        private readonly Lazy<FilesChangesClient> notifications;
        private readonly IFilesConflictListener[] conflictListeners;


        private bool resolvingConflict = false;
        private IDisposable failedUploadsObserver;

        private const int DefaultNumberOfCachedRequests = 2048;
        private static HttpJsonRequestFactory GetHttpJsonRequestFactory ()
        {
              return new HttpJsonRequestFactory(DefaultNumberOfCachedRequests);
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

        public AsyncFilesServerClient(string serverUrl, string fileSystemName, FilesConvention conventions, OperationCredentials credentials, HttpJsonRequestFactory requestFactory, Guid? sessionId, IFilesConflictListener[] conflictListeners, NameValueCollection operationsHeaders = null)
            : base(serverUrl, conventions, credentials, requestFactory, sessionId, operationsHeaders)
        {
            try
            {                
                this.FileSystem = fileSystemName;
                this.ApiKey = credentials.ApiKey;
                this.conflictListeners = conflictListeners ?? new IFilesConflictListener[0];

                notifications = new Lazy<FilesChangesClient>( () => new FilesChangesClient(BaseUrl, ApiKey, credentials.Credentials, RequestFactory, this.Conventions, this.ReplicationInformer, TryResolveConflictByUsingRegisteredListenersAsync, () => { }));

                InitializeSecurity();
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }
        }

        public AsyncFilesServerClient(string serverUrl, string fileSystemName, ICredentials credentials = null, string apiKey = null)
            : this(serverUrl, fileSystemName, new FilesConvention(), new OperationCredentials(apiKey, credentials ?? CredentialCache.DefaultNetworkCredentials), GetHttpJsonRequestFactory(), null, null, new NameValueCollection())
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
            return new AsyncFilesServerClient(this.ServerUrl, fileSystem, Conventions, PrimaryCredentials, RequestFactory, SessionId, this.conflictListeners, OperationsHeaders);
        }

        public IAsyncFilesCommands With(ICredentials credentials)
        {
            var primaryCredentials = new OperationCredentials(this.ApiKey, credentials);
            return new AsyncFilesServerClient(this.ServerUrl, this.FileSystem, Conventions, primaryCredentials, RequestFactory, SessionId, this.conflictListeners, OperationsHeaders);
        }
        public IAsyncFilesCommands With(OperationCredentials credentials)
        {
            return new AsyncFilesServerClient(this.ServerUrl, this.FileSystem, Conventions, credentials, RequestFactory, SessionId, this.conflictListeners, OperationsHeaders);
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

            Conventions.HandleUnauthorizedResponseAsync = async (unauthorizedResponse, credentials) =>
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
                    return await basicAuthenticator.HandleOAuthResponseAsync(oauthSource, credentials.ApiKey).ConfigureAwait(false);
                }

                if (credentials.ApiKey == null)
                {
                    AssertUnauthorizedCredentialSupportWindowsAuth(unauthorizedResponse, credentials.Credentials);
                    return null;
                }

                if (string.IsNullOrEmpty(oauthSource))
                    oauthSource = ServerUrl + "/OAuth/API-Key";

                return await securedAuthenticator.DoOAuthRequestAsync(ServerUrl, oauthSource, credentials.ApiKey).ConfigureAwait(false);
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
	            using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "GET", operation.Credentials, Conventions)).AddOperationHeaders(OperationsHeaders))
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
            return ExecuteWithReplication("DELETE", async operation =>
            {
                var requestUriString = operation.Url + "/files/" + Uri.EscapeDataString(filename);

	            using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "DELETE", operation.Credentials, Conventions)).AddOperationHeaders(OperationsHeaders))
	            {
		            AddEtagHeader(request, etag);
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
            return ExecuteWithReplication("PATCH", async operation =>
            {
                var requestUriString = operation.Url + "/files/" + Uri.EscapeDataString(filename) + "?rename=" + Uri.EscapeDataString(rename);

	            using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "PATCH", operation.Credentials, Conventions)).AddOperationHeaders(OperationsHeaders))
	            {
					AddEtagHeader(request, etag);
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
            return ExecuteWithReplication("GET", async operation =>
            {
	            using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operation.Url + "/files?start=" + start + "&pageSize=" + pageSize, "GET", operation.Credentials, Conventions)).AddOperationHeaders(OperationsHeaders))
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

	    public Task<string[]> GetSearchFieldsAsync(int start = 0, int pageSize = 25)
        {
            return ExecuteWithReplication("GET", async operation =>
            {
                var requestUriString = string.Format("{0}/search/terms?start={1}&pageSize={2}", operation.Url, start, pageSize);
	            using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "GET", operation.Credentials, Conventions).AddOperationHeaders(OperationsHeaders)))
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

	            using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriBuilder.ToString(), "GET", operation.Credentials, Conventions)).AddOperationHeaders(OperationsHeaders))
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

		public Task DeleteByQueryAsync(string query, string[] orderByFields = null, int start = 0, int pageSize = int.MaxValue)
		{
			return ExecuteWithReplication("DELETE", async operation =>
			{
				var requestUriBuilder = new StringBuilder(operation.Url)
					.Append("/search/?query=")
					.Append(Uri.EscapeUriString(query))
					.Append("&start=")
					.Append(start)
					.Append("&pageSize=")
					.Append(pageSize);

				if (orderByFields != null)
				{
					foreach (var sortField in orderByFields)
					{
						requestUriBuilder.Append("&sort=").Append(sortField);
					}
				}

				using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriBuilder.ToString(), "DELETE", operation.Credentials, Conventions)).AddOperationHeaders(OperationsHeaders))
				{
					try
					{
						await request.ReadResponseJsonAsync().ConfigureAwait(false);
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
            return ExecuteWithReplication("HEAD", operation => GetMetadataForAsyncImpl(filename, operation));
        }

        public Task<FileHeader[]> GetAsync(string[] filename)
        {
            return ExecuteWithReplication("GET", operation => GetAsyncImpl(filename, operation));
        }

	    public Task<FileHeader[]> StartsWithAsync(string prefix, string matches, int start, int pageSize)
	    {
			return ExecuteWithReplication("GET", operation => StartsWithAsyncImpl(prefix, matches, start, pageSize, operation));
	    }

	    public async Task<IAsyncEnumerator<FileHeader>> StreamFileHeadersAsync(Etag fromEtag, int pageSize = int.MaxValue)
        {
            if (fromEtag == null)
                throw new ArgumentException("fromEtag");

            var operationMetadata = new OperationMetadata(this.BaseUrl, this.CredentialsThatShouldBeUsedOnlyInOperationsWithoutReplication);

            if ( pageSize != int.MaxValue )
            {
                return new EtagStreamResults(this, fromEtag, pageSize);
            }
            else
            {
                var sb = new StringBuilder(operationMetadata.Url)
					.Append("/streams/files?etag=")
					.Append(fromEtag)
					.Append("&pageSize=")
					.Append(pageSize);

                var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, sb.ToString(), "GET", operationMetadata.Credentials, this.Conventions)
                                            .AddOperationHeaders(OperationsHeaders));

                var response = await request.ExecuteRawResponseAsync()
                                            .ConfigureAwait(false);

                await response.AssertNotFailingResponse();

                return new YieldStreamResults(await response.GetResponseStreamWithHttpDecompression().ConfigureAwait(false));
            }
        }

        internal class EtagStreamResults : IAsyncEnumerator<FileHeader>
        {
            private readonly AsyncFilesServerClient client;
            private readonly OperationMetadata operationMetadata;
            private readonly NameValueCollection headers;
            private readonly Etag startEtag;
            private readonly int pageSize;
            private readonly FilesConvention conventions;
            private readonly HttpJsonRequestFactory requestFactory;

            private bool complete;
            private bool wasInitialized;

            private FileHeader current;
            private YieldStreamResults currentStream;

            private Etag currentEtag;
            private int currentPageCount = 0;


            public EtagStreamResults(AsyncFilesServerClient client, Etag startEtag, int pageSize)
            {
                this.client = client;
                this.startEtag = startEtag;
                this.pageSize = pageSize;

                this.requestFactory = client.RequestFactory;
                this.operationMetadata = new OperationMetadata(client.BaseUrl, client.CredentialsThatShouldBeUsedOnlyInOperationsWithoutReplication);
                this.headers = client.OperationsHeaders;
                this.conventions = client.Conventions;
            }

            private async Task RequestPage(Etag etag)
            {
                if (currentStream != null)
                    currentStream.Dispose();

                var sb = new StringBuilder(operationMetadata.Url)
                       .Append("/streams/files?etag=")
                       .Append(etag)
                       .Append("&pageSize=")
                       .Append(pageSize);

                var request = requestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(client, sb.ToString(), "GET", operationMetadata.Credentials, conventions)
                                            .AddOperationHeaders(headers));

                var response = await request.ExecuteRawResponseAsync()
                                            .ConfigureAwait(false);

                await response.AssertNotFailingResponse();

                currentPageCount = 0;
                currentEtag = etag;
                currentStream = new YieldStreamResults(await response.GetResponseStreamWithHttpDecompression().ConfigureAwait(false));
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
                    await RequestPage(startEtag).ConfigureAwait(false);
                    wasInitialized = true;
                }

                if (await currentStream.MoveNextAsync().ConfigureAwait(false) == false)
                {
                    // We didn't finished the page, so there is no more data to retrieve.
                    if (currentPageCount < pageSize)
                    {
                        complete = true;
                        return false;
                    }
                    else
                    {
                        await RequestPage(current.Etag).ConfigureAwait(false);
                        return await this.MoveNextAsync().ConfigureAwait(false);
                    }
                }
                else
                {
                    current = currentStream.Current;
                    currentPageCount++;

                    return true;
                }                
            }

            public FileHeader Current
            {
                get { return current; }
            }

            public void Dispose()
            {
                if (currentStream != null)
                    currentStream.Dispose();
            }
        }

        internal class YieldStreamResults : IAsyncEnumerator<FileHeader>
        {
            private readonly Stream stream;
            private readonly StreamReader streamReader;
            private readonly JsonTextReaderAsync reader;
            private bool complete;

            private bool wasInitialized;

            public YieldStreamResults(Stream stream)
            {
                this.stream = stream;
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
                streamReader.Close();
                stream.Close();
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

			using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operation.Url + uri, "GET", operation.Credentials, Conventions)).AddOperationHeaders(OperationsHeaders))
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
            for( int i = 0; i < filenames.Length; i++ )
            {
                requestUriBuilder.Append("fileNames=" + Uri.EscapeDataString(filenames[i]));
                if (i < filenames.Length - 1)
                    requestUriBuilder.Append("&");
            }

	        using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operation.Url + requestUriBuilder.ToString(), "GET", operation.Credentials, Conventions)).AddOperationHeaders(OperationsHeaders))
	        {
				try
				{
					var response = (RavenJArray) await request.ReadResponseJsonAsync().ConfigureAwait(false);

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

        private async Task<RavenJObject> GetMetadataForAsyncImpl(string filename, OperationMetadata operation)
        {
	        using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operation.Url + "/files?name=" + Uri.EscapeDataString(filename), "HEAD", operation.Credentials, Conventions)).AddOperationHeaders(OperationsHeaders))
	        {
				try
				{
					await request.ExecuteRequestAsync().ConfigureAwait(false);

					var response = request.Response;

					var metadata = response.HeadersToObject();
					metadata[Constants.MetadataEtagField] = metadata[Constants.MetadataEtagField].Value<string>().Trim('\"');
					return metadata;
				}
				catch (Exception e)
				{
					try
					{
						throw e.SimplifyException();
					}
					catch (FileNotFoundException)
					{
						return null;
					}
				}
	        }
        }

        public Task<Stream> DownloadAsync(string filename, Reference<RavenJObject> metadataRef = null, long? from = null, long? to = null)
        {
            return ExecuteWithReplication("GET", async operation => await DownloadAsyncImpl("/files/", filename, metadataRef, from, to, operation).ConfigureAwait(false));
        }

        private async Task<Stream> DownloadAsyncImpl(string path, string filename, Reference<RavenJObject> metadataRef, long? @from, long? to, OperationMetadata operation)
        {
	        var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operation.Url + path + filename, "GET", operation.Credentials, Conventions)).AddOperationHeaders(OperationsHeaders);

			if (@from != null)
			{
				if (to != null)
					request.AddRange(@from.Value, to.Value);
				else
					request.AddRange(@from.Value);
			}

			try
			{
				var response = await request.ExecuteRawResponseAsync().ConfigureAwait(false);
				if (response.StatusCode == HttpStatusCode.NotFound)
					throw new FileNotFoundException("The file requested does not exists on the file system.", operation.Url + path + filename);

				await response.AssertNotFailingResponse().ConfigureAwait(false);

				if (metadataRef != null)
					metadataRef.Value = response.HeadersToObject();

				return await response.GetResponseStreamWithHttpDecompression().ConfigureAwait(false);
			}
			catch (Exception e)
			{
				throw e.SimplifyException();
			}
        }

        public Task UpdateMetadataAsync(string filename, RavenJObject metadata, Etag etag = null)
        {
            return ExecuteWithReplication("POST", async operation =>
            {
	            using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operation.Url + "/files/" + filename, "POST", operation.Credentials, Conventions)).AddOperationHeaders(OperationsHeaders))
	            {
					AddHeaders(metadata, request);
					AddEtagHeader(request, etag);
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

        public Task UploadAsync(string filename, Stream source, RavenJObject metadata = null, long? size = null, Etag etag = null)
        {
            if (metadata == null)
                metadata = new RavenJObject();

            return ExecuteWithReplication("PUT", async operation =>
            {
                await UploadAsyncImpl(operation, filename, source, metadata, false, size, etag).ConfigureAwait(false);
            });
        }

        public Task UploadRawAsync(string filename, Stream source, RavenJObject metadata, long size, Etag etag = null)
        {
            var operationMetadata = new OperationMetadata(this.BaseUrl, this.CredentialsThatShouldBeUsedOnlyInOperationsWithoutReplication);
            return UploadAsyncImpl(operationMetadata, filename, source, metadata, true, size, etag);
        }

        private async Task UploadAsyncImpl(OperationMetadata operation, string filename, Stream source, RavenJObject metadata, bool preserveTimestamps, long? size, Etag etag)
        {
            if (source.CanRead == false)
                throw new Exception("Stream does not support reading");

            var uploadIdentifier = Guid.NewGuid();

            var operationUrl = operation.Url + "/files?name=" + Uri.EscapeDataString(filename) + "&uploadId=" + uploadIdentifier;
            if (preserveTimestamps)
                operationUrl += "&preserveTimestamps=true";

	        using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationUrl, "PUT", operation.Credentials, Conventions)).AddOperationHeaders(OperationsHeaders))
	        {
				metadata[Constants.FileSystem.RavenFsSize] = size.HasValue ? new RavenJValue(size.Value) : new RavenJValue(source.Length);

		        AddHeaders(metadata, request);
				AddEtagHeader(request, etag);

		        var cts = new CancellationTokenSource();

		        RegisterUploadOperation(uploadIdentifier, cts);

		        try
		        {
			        await request.WriteAsync(source).ConfigureAwait(false);
			        if (request.ResponseStatusCode == HttpStatusCode.BadRequest) throw new BadRequestException("There is a mismatch between the size reported in the RavenFS-Size header and the data read server side.");
		        }
		        catch (Exception e)
		        {
			        throw e.SimplifyException();
		        }
		        finally
		        {
			        UnregisterUploadOperation(uploadIdentifier);
		        }
	        }
        }

        internal async Task<bool> TryResolveConflictByUsingRegisteredListenersAsync(string filename, FileHeader remote, string sourceServerUri, Action beforeConflictResolution)
        {
            var files = await this.GetAsync(new[] { filename });
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
                        var client = new SynchronizationClient(this, this.Conventions);
                        await client.ResolveConflictAsync(filename, resolutionStrategy);

                        // Refreshing the file information.
                        files = await this.GetAsync(new[] { filename });                        
                        files.ApplyIfNotNull ( x => 
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

	    private static void AddEtagHeader(HttpJsonRequest request, Etag etag)
	    {
		    if (etag != null)
		    {
				request.AddHeader("If-None-Match", "\"" + etag + "\"");
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

	            using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "GET", operation.Credentials, Conventions)).AddOperationHeaders(OperationsHeaders))
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
            return ExecuteWithReplication("GET", async operation =>
            {
                var requestUriString = operation.Url + "/static/id";

	            using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "GET", operation.Credentials, Conventions)).AddOperationHeaders(OperationsHeaders))
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

	                using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "GET", operation.Credentials, convention)).AddOperationHeaders(client.OperationsHeaders))
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
                return client.ExecuteWithReplication("PUT", async operation =>
                {
                    var requestUriString = operation.Url + "/config?name=" + Uri.EscapeDataString(name);
	                using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "PUT", operation.Credentials, convention)).AddOperationHeaders(client.OperationsHeaders))
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
                return client.ExecuteWithReplication("DELETE", async operation =>
                {
                    var requestUriString = operation.Url + "/config?name=" + Uri.EscapeDataString(name);

	                using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "DELETE", operation.Credentials, convention)).AddOperationHeaders(client.OperationsHeaders))
	                {
						await request.ExecuteRequestAsync().ConfigureAwait(false);
	                }
                });
            }

            public Task<T> GetKeyAsync<T>(string name)
            {
                return client.ExecuteWithReplication("GET", async operation =>
                {
                    var requestUriString = operation.Url + "/config?name=" + Uri.EscapeDataString(name);

	                using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "GET", operation.Credentials, convention)).AddOperationHeaders(client.OperationsHeaders))
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
                return client.ExecuteWithReplication("GET", async operation =>
                {
                    var requestUriBuilder = new StringBuilder(operation.Url)
                        .Append("/config/search/?prefix=")
                        .Append(Uri.EscapeUriString(prefix))
                        .Append("&start=")
                        .Append(start)
                        .Append("&pageSize=")
                        .Append(pageSize);

	                using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriBuilder.ToString(), "GET", operation.Credentials, convention)).AddOperationHeaders(client.OperationsHeaders))
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

            public async Task DownloadSignatureAsync(string sigName, Stream destination, long? from = null, long? to = null)
            {                
                var stream = await client.DownloadAsyncImpl("/rdc/signatures/", sigName, null, from, to, new OperationMetadata(client.BaseUrl, credentials));
                await stream.CopyToAsync(destination);
            }

            public async Task<SignatureManifest> GetRdcManifestAsync(string path)
            {
                var requestUriString = client.BaseUrl + "/rdc/manifest/" + Uri.EscapeDataString(path);
	            using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "GET", credentials, convention)).AddOperationHeaders(client.OperationsHeaders))
	            {
					try
					{
						var response = (RavenJObject)await request.ReadResponseJsonAsync().ConfigureAwait(false);
						return response.JsonDeserialization<SignatureManifest>();
					}
					catch (Exception e)
					{
						throw e.SimplifyException();
					}
	            }
            }

            public async Task<DestinationSyncResult[]> SynchronizeAsync(bool forceSyncingAll = false)
            {
                var requestUriString = String.Format("{0}/synchronization/ToDestinations?forceSyncingAll={1}", client.BaseUrl, forceSyncingAll);

	            using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "POST", credentials, convention)).AddOperationHeaders(client.OperationsHeaders))
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

            public Task<SynchronizationDestination[]> GetDestinationsAsync()
            {
                return client.ExecuteWithReplication("GET", async operation =>
                {
                    var requestUriString = operation.Url + "/config?name=" + Uri.EscapeDataString(SynchronizationConstants.RavenSynchronizationDestinations);
	                using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "GET", operation.Credentials, convention)).AddOperationHeaders(client.OperationsHeaders))
	                {
						try
						{
							var response = (RavenJObject)await request.ReadResponseJsonAsync().ConfigureAwait(false);
							var rawDestinations = (RavenJArray)response["Destinations"];
							return rawDestinations.JsonDeserialization<SynchronizationDestination>();
						}
						catch (Exception e)
						{
							throw e.SimplifyException();
						}
	                }
                });
            }

            public Task SetDestinationsAsync(params SynchronizationDestination[] destinations)
            {
                return client.ExecuteWithReplication("PUT", async operation =>
                {
                    var requestUriString = operation.Url + "/config?name=" + Uri.EscapeDataString(SynchronizationConstants.RavenSynchronizationDestinations);
	                using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "PUT", operation.Credentials, convention)).AddOperationHeaders(client.OperationsHeaders))
	                {
						var data = new { Destinations = destinations };

						try
						{
							await request.WriteWithObjectAsync(data).ConfigureAwait(false);
						}
						catch (Exception e)
						{
							throw e.SimplifyException();
						}
	                }
                });
            }

            public async Task<SynchronizationReport> StartAsync(string fileName, SynchronizationDestination destination)
            {
                var requestUriString = String.Format("{0}/synchronization/start/{1}", client.BaseUrl, Uri.EscapeDataString(fileName));

	            using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "POST", credentials, convention)).AddOperationHeaders(client.OperationsHeaders))
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

            public async Task<SynchronizationReport> GetSynchronizationStatusForAsync(string fileName)
            {
                var requestUriString = String.Format("{0}/synchronization/status/{1}", client.BaseUrl, Uri.EscapeDataString(fileName));

	            using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "GET", credentials, convention)).AddOperationHeaders(client.OperationsHeaders))
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

            public async Task ResolveConflictAsync(string filename, ConflictResolutionStrategy strategy)
            {
                var requestUriString = String.Format("{0}/synchronization/resolveConflict/{1}?strategy={2}",
                                                        client.BaseUrl, Uri.EscapeDataString(filename),
                                                        Uri.EscapeDataString(strategy.ToString()));

	            using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "PATCH", credentials, convention)).AddOperationHeaders(client.OperationsHeaders))
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

            public async Task ApplyConflictAsync(string filename, long remoteVersion, string remoteServerId,
                                                   RavenJObject remoteMetadata, string remoteServerUrl)
            {
                var requestUriString =
                    String.Format("{0}/synchronization/applyConflict/{1}?remoteVersion={2}&remoteServerId={3}&remoteServerUrl={4}",
                                  client.BaseUrl, Uri.EscapeDataString(filename), remoteVersion,
                                  Uri.EscapeDataString(remoteServerId), Uri.EscapeDataString(remoteServerUrl));

	            using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "PATCH", credentials, convention)).AddOperationHeaders(client.OperationsHeaders))
	            {
					try
					{
						await request.WriteAsync(remoteMetadata).ConfigureAwait(false);
					}
					catch (Exception e)
					{
						throw e.SimplifyException();
					}
	            }
            }

			public async Task<ConflictResolutionStrategy> GetResolutionStrategyFromDestinationResolvers(ConflictItem conflict, RavenJObject localMetadata)
	        {
		        var requestUriString = string.Format("{0}/synchronization/ResolutionStrategyFromServerResolvers", client.BaseUrl);

				using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "POST", credentials, convention)).AddOperationHeaders(client.OperationsHeaders))
				{
					request.AddHeaders(localMetadata);

					try
					{
						await request.WriteWithObjectAsync(conflict).ConfigureAwait(false);
						var response = await request.ReadResponseJsonAsync().ConfigureAwait(false);
						return response.JsonDeserialization<ConflictResolutionStrategy>();
					}
					catch (Exception e)
					{
						throw e.SimplifyException();
					}
				}
	        }

            public async Task<ItemsPage<SynchronizationReport>> GetFinishedAsync(int page = 0, int pageSize = 25)
            {
                var requestUriString = String.Format("{0}/synchronization/finished?start={1}&pageSize={2}", client.BaseUrl, page,
                                                         pageSize);

	            using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "GET", credentials, convention)).AddOperationHeaders(client.OperationsHeaders))
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

            public async Task<ItemsPage<SynchronizationDetails>> GetActiveAsync(int page = 0, int pageSize = 25)
            {
                var requestUriString = String.Format("{0}/synchronization/active?start={1}&pageSize={2}",
                                                        client.BaseUrl, page, pageSize);

	            using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "GET", credentials, convention)).AddOperationHeaders(client.OperationsHeaders))
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

            public async Task<ItemsPage<SynchronizationDetails>> GetPendingAsync(int page = 0, int pageSize = 25)
            {
                var requestUriString = String.Format("{0}/synchronization/pending?start={1}&pageSize={2}",
                                                     client.BaseUrl, page, pageSize);

	            using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "GET", credentials, convention)).AddOperationHeaders(client.OperationsHeaders))
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

            public async Task<SourceSynchronizationInformation> GetLastSynchronizationFromAsync(Guid serverId)
            {
                var requestUriString = String.Format("{0}/synchronization/LastSynchronization?from={1}", client.BaseUrl, serverId);

	            using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "GET", credentials, convention)).AddOperationHeaders(client.OperationsHeaders))
	            {
					try
					{
						var response = (RavenJObject)await request.ReadResponseJsonAsync().ConfigureAwait(false);
						return response.JsonDeserialization<SourceSynchronizationInformation>();
					}
					catch (Exception e)
					{
						throw e.SimplifyException();
					}
	            }
            }

            public async Task<SynchronizationConfirmation[]> GetConfirmationForFilesAsync(IEnumerable<Tuple<string, Etag>> sentFiles)
            {
                var requestUriString = String.Format("{0}/synchronization/Confirm", client.BaseUrl);

	            using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "POST", credentials, convention)).AddOperationHeaders(client.OperationsHeaders))
	            {
					try
					{
						using (var stream = new MemoryStream())
						{
							var sb = new StringBuilder();
							var jw = new JsonTextWriter(new StringWriter(sb));
							JsonExtensions.CreateDefaultJsonSerializer().Serialize(jw, sentFiles);
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
            }

            public async Task<ItemsPage<ConflictItem>> GetConflictsAsync(int page = 0, int pageSize = 25)
            {
                var requestUriString = String.Format("{0}/synchronization/conflicts?start={1}&pageSize={2}", client.BaseUrl, page,
                                                         pageSize);

	            using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "GET", credentials, convention)).AddOperationHeaders(client.OperationsHeaders))
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

            public async Task IncrementLastETagAsync(Guid sourceServerId, string sourceFileSystemUrl, Etag sourceFileETag)
            {
                var requestUriString =
                    String.Format("{0}/synchronization/IncrementLastETag?sourceServerId={1}&sourceFileSystemUrl={2}&sourceFileETag={3}",
                                    client.BaseUrl, sourceServerId, sourceFileSystemUrl, sourceFileETag);

	            using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "POST", credentials, convention)).AddOperationHeaders(client.OperationsHeaders))
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

            public async Task<RdcStats> GetRdcStatsAsync()
            {
                var requestUriString = client.BaseUrl + "/rdc/stats";

	            using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "GET", credentials, convention)).AddOperationHeaders(client.OperationsHeaders))
	            {
					try
					{
						var response = (RavenJObject)await request.ReadResponseJsonAsync().ConfigureAwait(false);
						return response.JsonDeserialization<RdcStats>();
					}
					catch (Exception e)
					{
						throw e.SimplifyException();
					}
	            }
            }

            public async Task<SynchronizationReport> RenameAsync(string currentName, string newName, RavenJObject metadata, FileSystemInfo sourceFileSystem)
            {
	            using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, client.BaseUrl + "/synchronization/rename?filename=" + Uri.EscapeDataString(currentName) + "&rename=" + Uri.EscapeDataString(newName), "PATCH", credentials, convention)).AddOperationHeaders(client.OperationsHeaders))
	            {
					request.AddHeaders(metadata);
					request.AddHeader(SyncingMultipartConstants.SourceFileSystemInfo, sourceFileSystem.AsJson());
					AddEtagHeader(request, Etag.Parse(metadata.Value<string>(Constants.MetadataEtagField)));

					try
					{
						var response = (RavenJObject)await request.ReadResponseJsonAsync().ConfigureAwait(false);
						return response.JsonDeserialization<SynchronizationReport>();
					}
					catch (ErrorResponseException exception)
					{
						throw exception.SimplifyException();
					}
	            }
            }

            public async Task<SynchronizationReport> DeleteAsync(string fileName, RavenJObject metadata, FileSystemInfo sourceFileSystem)
            {
	            using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, client.BaseUrl + "/synchronization?fileName=" + Uri.EscapeDataString(fileName), "DELETE", credentials, convention)).AddOperationHeaders(client.OperationsHeaders))
	            {
					request.AddHeaders(metadata);
					request.AddHeader(SyncingMultipartConstants.SourceFileSystemInfo, sourceFileSystem.AsJson());
					AddEtagHeader(request, Etag.Parse(metadata.Value<string>(Constants.MetadataEtagField)));

					try
					{
						var response = (RavenJObject)await request.ReadResponseJsonAsync().ConfigureAwait(false);
						return response.JsonDeserialization<SynchronizationReport>();
					}
					catch (ErrorResponseException exception)
					{
						throw exception.SimplifyException();
					}
	            }
            }

            public async Task<SynchronizationReport> UpdateMetadataAsync(string fileName, RavenJObject metadata, FileSystemInfo sourceFileSystem)
            {
	            using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, client.BaseUrl + "/synchronization/UpdateMetadata/" + Uri.EscapeDataString(fileName), "POST", credentials, convention)).AddOperationHeaders(client.OperationsHeaders))
	            {
					request.AddHeaders(metadata);
					request.AddHeader(SyncingMultipartConstants.SourceFileSystemInfo, sourceFileSystem.AsJson());
					AddEtagHeader(request, Etag.Parse(metadata.Value<string>(Constants.MetadataEtagField)));

					try
					{
						var response = (RavenJObject)await request.ReadResponseJsonAsync().ConfigureAwait(false);
						return response.JsonDeserialization<SynchronizationReport>();
					}
					catch (ErrorResponseException exception)
					{
						throw exception.SimplifyException();
					}
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

	                using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "POST", operation.Credentials, convention)).AddOperationHeaders(client.OperationsHeaders))
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
                return client.ExecuteWithReplication("POST", async operation =>
                {
                    var requestUriString = String.Format("{0}/storage/retryrenaming", operation.Url);

	                using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "POST", operation.Credentials, convention)).AddOperationHeaders(client.OperationsHeaders))
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
                var requestUriString = string.Format("{0}/fs", client.ServerUrl);

	            using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "GET", client.PrimaryCredentials, convention)))
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

	            using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "GET", client.PrimaryCredentials, convention)))
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

            public async Task CreateFileSystemAsync(FileSystemDocument filesystemDocument, string newFileSystemName = null)
            {
				var requestUriString = string.Format("{0}/admin/fs/{1}", client.ServerUrl,
                                                     newFileSystemName ?? client.FileSystem);

	            using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "PUT", client.PrimaryCredentials, convention)))
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
                                                     newFileSystemName ?? client.FileSystem);

	            using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "PUT", client.PrimaryCredentials, convention)))
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
				var requestUriString = string.Format("{0}/admin/fs/{1}?hard-delete={2}", client.ServerUrl, fileSystemName ?? client.FileSystem, hardDelete);

				using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "DELETE", client.PrimaryCredentials, convention)))
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
                var filesystems = await GetNamesAsync();
                if (filesystems.Contains(fileSystem))
                    return;

                await CreateOrUpdateFileSystemAsync(
                    new FileSystemDocument
                    {
                        Id = "Raven/FileSystem/" + fileSystem,
                        Settings =
                        {
                            { Constants.FileSystem.DataDirectory, Path.Combine("~", Path.Combine("FileSystems", fileSystem))}
                        }
                    }, fileSystem);
            }

            public async Task<long> StartRestore(FilesystemRestoreRequest restoreRequest)
            {
                var requestUrlString = string.Format("{0}/admin/fs/restore", client.ServerUrl);

	            using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUrlString, "POST", client.PrimaryCredentials, convention)))
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
                var requestUrlString = string.Format("{0}/admin/fs/compact?filesystem={1}", client.ServerUrl, Uri.EscapeDataString(filesystemName));

	            using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUrlString, "POST", client.PrimaryCredentials, convention)))
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

            public async Task StartBackup(string backupLocation, FileSystemDocument databaseDocument, bool incremental, string filesystemName)
            {
                var requestUrlString = string.Format("{0}/fs/{1}/admin/fs/backup?incremental={2}", client.ServerUrl, filesystemName, incremental);

	            using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUrlString, "POST", client.PrimaryCredentials, convention)))
	            {
					try
					{
						await request.WriteWithObjectAsync(new FilesystemBackupRequest
						{
							BackupLocation = backupLocation,
							FileSystemDocument = databaseDocument
						}).ConfigureAwait(false);
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

                using (var request = client.RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUrlString, "POST", client.PrimaryCredentials, convention)))
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

        public override void Dispose()
        {            
            if (notifications.IsValueCreated)
                notifications.Value.Dispose();

            base.Dispose();
        }

        public ProfilingInformation ProfilingInformation { get; private set; }
    }
}