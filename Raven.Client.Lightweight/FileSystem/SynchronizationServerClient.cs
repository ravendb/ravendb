using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Connection.Profiling;
using Raven.Client.Extensions;
using Raven.Client.FileSystem.Extensions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using FileSystemInfo = Raven.Abstractions.FileSystem.FileSystemInfo;

namespace Raven.Client.FileSystem
{
    public class SynchronizationServerClient : ISynchronizationServerClient
    {
        private readonly string baseUrl;
        private readonly FilesConvention filesConvention;
        private readonly OperationCredentials operationCredentials;
        private readonly HttpJsonRequestFactory requestFactory;
        protected readonly NameValueCollection OperationsHeaders;

        public string BaseUrl
        {
            get { return baseUrl; }
        }

        public FilesConvention Conventions
        {
            get { return filesConvention; }
        }

        public OperationCredentials Credentials
        {
            get { return operationCredentials; }
        }

        public HttpJsonRequestFactory RequestFactory
        {
            get { return requestFactory; }
        }

        public SynchronizationServerClient(string serverUrl, string fileSystem, string apiKey, ICredentials credentials, HttpJsonRequestFactory requestFactory, FilesConvention convention, 
            OperationCredentials operationCredentials = null, NameValueCollection operationsHeaders = null)
        {
            serverUrl = serverUrl.TrimEnd('/');
            baseUrl = serverUrl + "/fs/" + Uri.EscapeDataString(fileSystem);
            credentials = credentials ?? CredentialCache.DefaultNetworkCredentials;
            filesConvention = convention ?? new FilesConvention();
            this.operationCredentials = operationCredentials ?? new OperationCredentials(apiKey, credentials);

            this.requestFactory = requestFactory;

            this.OperationsHeaders = operationsHeaders ?? new NameValueCollection();
        }

        public Task<RavenJObject> GetMetadataForAsync(string fileName)
        {
            return AsyncFilesServerClientExtension.GetMetadataForAsyncImpl(this, RequestFactory, Conventions, OperationsHeaders, fileName, baseUrl, Credentials);
        }

        public async Task DownloadSignatureAsync(string sigName, Stream destination, long? from = null, long? to = null)
        {
            var stream = await AsyncFilesServerClientExtension.DownloadAsyncImpl(this, RequestFactory, Conventions, OperationsHeaders, "/rdc/signatures/", sigName, null, from, to, BaseUrl, Credentials).ConfigureAwait(false);
            await stream.CopyToAsync(destination).ConfigureAwait(false);
        }

        public async Task<RdcStats> GetRdcStatsAsync()
        {
            var requestUriString = baseUrl + "/rdc/stats";

            using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethod.Get, Credentials, Conventions)).AddOperationHeaders(OperationsHeaders))
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
            using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, baseUrl + "/synchronization/rename?filename=" + Uri.EscapeDataString(currentName) + "&rename=" + Uri.EscapeDataString(newName), HttpMethods.Patch, Credentials, Conventions)).AddOperationHeaders(OperationsHeaders))
            {
                request.AddHeaders(metadata);
                request.AddHeader(SyncingMultipartConstants.SourceFileSystemInfo, sourceFileSystem.AsJson());
                AsyncFilesServerClientExtension.AddEtagHeader(request, Etag.Parse(metadata.Value<string>(Constants.MetadataEtagField)));

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
            using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, baseUrl + "/synchronization?fileName=" + Uri.EscapeDataString(fileName), HttpMethod.Delete, Credentials, Conventions)).AddOperationHeaders(OperationsHeaders))
            {
                request.AddHeaders(metadata);
                request.AddHeader(SyncingMultipartConstants.SourceFileSystemInfo, sourceFileSystem.AsJson());
                AsyncFilesServerClientExtension.AddEtagHeader(request, Etag.Parse(metadata.Value<string>(Constants.MetadataEtagField)));

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
            using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, baseUrl + "/synchronization/UpdateMetadata/" + Uri.EscapeDataString(fileName), HttpMethod.Post, Credentials, Conventions)).AddOperationHeaders(OperationsHeaders))
            {
                request.AddHeaders(metadata);
                request.AddHeader(SyncingMultipartConstants.SourceFileSystemInfo, sourceFileSystem.AsJson());
                AsyncFilesServerClientExtension.AddEtagHeader(request, Etag.Parse(metadata.Value<string>(Constants.MetadataEtagField)));

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

        public async Task<SourceSynchronizationInformation> GetLastSynchronizationFromAsync(Guid serverId)
        {
            var requestUriString = string.Format("{0}/synchronization/LastSynchronization?from={1}", BaseUrl, serverId);

            using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethod.Get, Credentials, Conventions)).AddOperationHeaders(OperationsHeaders))
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

        public async Task ResolveConflictAsync(string filename, ConflictResolutionStrategy strategy)
        {
            var requestUriString = string.Format("{0}/synchronization/resolveConflict/{1}?strategy={2}",
                baseUrl, Uri.EscapeDataString(filename),
                Uri.EscapeDataString(strategy.ToString()));

            using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethods.Patch, Credentials, Conventions)).AddOperationHeaders(OperationsHeaders))
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

        public async Task ResolveConflictsAsync(ConflictResolutionStrategy strategy)
        {
            var requestUriString = string.Format("{0}/synchronization/resolveConflicts?strategy={1}",
                baseUrl, Uri.EscapeDataString(strategy.ToString()));

            using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethods.Patch, Credentials, Conventions)).AddOperationHeaders(OperationsHeaders))
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
                string.Format("{0}/synchronization/applyConflict/{1}?remoteVersion={2}&remoteServerId={3}&remoteServerUrl={4}",
                    baseUrl, Uri.EscapeDataString(filename), remoteVersion,
                    Uri.EscapeDataString(remoteServerId), Uri.EscapeDataString(remoteServerUrl));

            using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethods.Patch, Credentials, Conventions)).AddOperationHeaders(OperationsHeaders))
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
            var requestUriString = string.Format("{0}/synchronization/ResolutionStrategyFromServerResolvers", baseUrl);

            using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethods.Post, Credentials, Conventions)).AddOperationHeaders(OperationsHeaders))
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

        public async Task<SynchronizationConfirmation[]> GetConfirmationForFilesAsync(IEnumerable<Tuple<string, Etag>> sentFiles)
        {
            var requestUriString = string.Format("{0}/synchronization/Confirm", baseUrl);

            using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethods.Post, Credentials, Conventions)).AddOperationHeaders(OperationsHeaders))
            {
                try
                {
                    using (var stream = new MemoryStream())
                    {
                        var sb = new StringBuilder();
                        var jw = new JsonTextWriter(new StringWriter(sb));
                        JsonExtensions.CreateDefaultJsonSerializer().Serialize(jw, sentFiles);
                        var bytes = Encoding.UTF8.GetBytes(sb.ToString());

                        await stream.WriteAsync(bytes, 0, bytes.Length).ConfigureAwait(false);
                        stream.Position = 0;
                        await request.WriteAsync(stream).ConfigureAwait(false);

                        var response = (RavenJArray)await request.ReadResponseJsonAsync().ConfigureAwait(false);
                        return response.JsonDeserialization<SynchronizationConfirmation>();
                    }

                }
                catch (Exception e)
                {
                    throw e.SimplifyException();
                }
            }
        }

        public async Task<SignatureManifest> GetRdcManifestAsync(string path)
        {
            var requestUriString = baseUrl + "/rdc/manifest/" + Uri.EscapeDataString(path);
            using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethods.Get, Credentials, Conventions)).AddOperationHeaders(OperationsHeaders))
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

        public async Task IncrementLastETagAsync(Guid sourceServerId, string sourceFileSystemUrl, Etag sourceFileETag)
        {
            var requestUriString =
                string.Format("{0}/synchronization/IncrementLastETag?sourceServerId={1}&sourceFileSystemUrl={2}&sourceFileETag={3}",
                    baseUrl, sourceServerId, sourceFileSystemUrl, sourceFileETag);

            using (var request = RequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethods.Post, Credentials, Conventions)).AddOperationHeaders(OperationsHeaders))
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
    }
}
