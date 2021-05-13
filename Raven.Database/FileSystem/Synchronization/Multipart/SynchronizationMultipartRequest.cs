using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Client.Connection;
using Raven.Client.Connection.Profiling;
using Raven.Client.FileSystem;
using Raven.Client.FileSystem.Extensions;
using Raven.Database.FileSystem.Synchronization.Rdc.Wrapper;
using Raven.Database.FileSystem.Util;
using Raven.Json.Linq;

using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using FileSystemInfo = Raven.Abstractions.FileSystem.FileSystemInfo;

namespace Raven.Database.FileSystem.Synchronization.Multipart
{
    internal class SynchronizationMultipartRequest : IHoldProfilingInformation
    {
        private readonly ISynchronizationServerClient synchronizationServerClient;
        private readonly string fileName;
        private readonly IList<RdcNeed> needList;
        private readonly SynchronizationType synchronizationType;
        private readonly FileSystemInfo fileSystemInfo;
        private readonly RavenJObject sourceMetadata;
        private readonly Stream sourceStream;
        private readonly string syncingBoundary;

        public SynchronizationMultipartRequest(ISynchronizationServerClient synchronizationServerClient, FileSystemInfo fileSystemInfo, string fileName, RavenJObject sourceMetadata, Stream sourceStream, IList<RdcNeed> needList, SynchronizationType synchronizationType)
        {
            this.synchronizationServerClient = synchronizationServerClient;
            this.fileSystemInfo = fileSystemInfo;
            this.fileName = fileName;
            this.sourceMetadata = sourceMetadata;
            this.sourceStream = sourceStream;
            this.needList = needList;
            this.synchronizationType = synchronizationType;
            syncingBoundary = "syncing";
        }

        public async Task<SynchronizationReport> PushChangesAsync(CancellationToken token)
        {
            token.Register(() => { });//request.Abort() TODO: check this

            token.ThrowIfCancellationRequested();

            if (sourceStream.CanRead == false)
                throw new Exception("Stream does not support reading");

            var baseUrl = synchronizationServerClient.BaseUrl;
            var credentials = synchronizationServerClient.Credentials;
            var conventions = synchronizationServerClient.Conventions;

            var url = baseUrl + "/synchronization/MultipartProceed";

            if (synchronizationType == SynchronizationType.ContentUpdateNoRDC)
                url += "?skip-rdc=true";

            var requestParams = new CreateHttpJsonRequestParams(this, url, HttpMethod.Post, credentials, conventions, timeout: TimeSpan.FromHours(12))
            {
                DisableRequestCompression = true
            };

            using (var request = synchronizationServerClient.RequestFactory.CreateHttpJsonRequest(requestParams))
            {
                request.AddHeaders(sourceMetadata);
                request.AddHeader("Content-Type", "multipart/form-data; boundary=" + syncingBoundary);
                request.AddHeader("If-None-Match", "\"" + sourceMetadata.Value<string>(Constants.MetadataEtagField) + "\"");

                request.AddHeader(SyncingMultipartConstants.FileName, Uri.EscapeDataString(fileName));
                request.AddHeader(SyncingMultipartConstants.SourceFileSystemInfo, fileSystemInfo.AsJson());

                try
                {
                    await request.WriteAsync(PrepareMultipartContent(token)).ConfigureAwait(false);

                    var response = await request.ReadResponseJsonAsync().ConfigureAwait(false);
                    return JsonExtensions.CreateDefaultJsonSerializer().Deserialize<SynchronizationReport>(new RavenJTokenReader(response));
                }
                catch (Exception exception)
                {
                    if (token.IsCancellationRequested)
                    {
                        throw new OperationCanceledException(token);
                    }

                    var webException = exception as ErrorResponseException;

                    if (webException != null)
                    {
                        webException.SimplifyException();
                    }

                    throw;
                }
            }
        }

        internal MultipartContent PrepareMultipartContent(CancellationToken token)
        {
            var content = new MultipartContent("form-data", syncingBoundary);

            foreach (var item in needList)
            {
                token.ThrowIfCancellationRequested();

                var @from = Convert.ToInt64(item.FileOffset);
                var length = Convert.ToInt64(item.BlockLength);
                var to = from + length - 1;

                switch (item.BlockType)
                {
                    case RdcNeedType.Source:
                        content.Add(new SourceFilePart(new NarrowedStream(sourceStream, from, to)));
                        break;
                    case RdcNeedType.Seed:
                        content.Add(new SeedFilePart(@from, to));
                        break;
                    default:
                        throw new NotSupportedException();
                }
            }

            return content;
        }

        public ProfilingInformation ProfilingInformation { get; private set; }
    }
}
