using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;
using Raven.Abstractions.Util;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Client.FileSystem;
using Raven.Client.FileSystem.Connection;
using Raven.Client.FileSystem.Extensions;
using Raven.Client.Util;
using Raven.Json.Linq;

namespace Raven.Smuggler
{
    public class SmugglerRemoteFilesOperations : ISmugglerFilesOperations
    {
        private readonly Func<FilesStore> primaryStore;
        private readonly Func<DocumentStore> documentStore;

        const int RetriesCount = 5;

        protected FilesStore PrimaryStore
        {
            get { return primaryStore(); }
        }

        protected DocumentStore DocumentStore
        {
            get { return documentStore(); }
        }        


        public SmugglerFilesOptions Options { get; private set; }

        public bool LastRequestErrored { get; set; }

        public SmugglerRemoteFilesOperations(Func<FilesStore> primaryStore, Func<DocumentStore> documentStore)
        {
            if (primaryStore == null)
                throw new ArgumentNullException("primaryStore");

            if (documentStore == null)
                throw new ArgumentNullException("documentStore");

            this.primaryStore = primaryStore;
            this.documentStore = documentStore;
        }

        public virtual async Task<FileSystemStats[]> GetStats()
        {
            return await PrimaryStore.AsyncFilesCommands.Admin.GetStatisticsAsync().ConfigureAwait(false);
        }

        public virtual Task<BuildNumber> GetVersion(FilesConnectionStringOptions server)
        {
            return DocumentStore.AsyncDatabaseCommands.GlobalAdmin.GetBuildNumberAsync();
        }

        public virtual LastFilesEtagsInfo FetchCurrentMaxEtags()
        {
            return new LastFilesEtagsInfo
            { 
                LastFileEtag = null,
                LastDeletedFileEtag = null
            };
        }

        public virtual async Task<IAsyncEnumerator<FileHeader>> GetFiles(Etag lastEtag, int take)
        {
            ShowProgress("Streaming documents from {0}, batch size {1}", lastEtag, take);
            return await PrimaryStore.AsyncFilesCommands.StreamFileHeadersAsync(lastEtag, pageSize: take).ConfigureAwait(false);
        }
        
        public virtual Task<Stream> DownloadFile(FileHeader file)
        {
        
            return PrimaryStore.AsyncFilesCommands.DownloadAsync(file.FullPath);
        }

        public virtual Task PutFile(FileHeader file, Stream data, long size)
        {
            return PrimaryStore.AsyncFilesCommands.UploadRawAsync(file.FullPath, data, file.Metadata, size);
        }

        public virtual async Task<IEnumerable<KeyValuePair<string, RavenJObject>>> GetConfigurations(int start, int take)
        {
            var names = await PrimaryStore.AsyncFilesCommands.Configuration.GetKeyNamesAsync(start, take).ConfigureAwait(false);

            var results = new List<KeyValuePair<string, RavenJObject>>(names.Length);

            foreach (var name in names)
            {
                results.Add(new KeyValuePair<string, RavenJObject>(name, await PrimaryStore.AsyncFilesCommands.Configuration.GetKeyAsync<RavenJObject>(name).ConfigureAwait(false)));
            }

            return results;
        }

        public virtual Task PutConfig(string name, RavenJObject value)
        {
            return PrimaryStore.AsyncFilesCommands.Configuration.SetKeyAsync(name, value);
        }

        public virtual void Initialize(SmugglerFilesOptions options)
        {
            this.Options = options;
        }

        public virtual void Configure(SmugglerFilesOptions options)
        {
        }

        public virtual void ShowProgress(string format, params object[] args)
        {
            try
            {
                Console.WriteLine(format, args);
            }
            catch (FormatException e)
            {
                throw new FormatException("Input string is invalid: " + format + Environment.NewLine + string.Join(", ", args), e);
            }
        }


        public virtual string CreateIncrementalKey()
        {
            throw new NotSupportedException();
        }

        public virtual Task<ExportFilesDestinations> GetIncrementalExportKey()
        {
            throw new NotSupportedException();
        }

        public virtual Task PutIncrementalExportKey(ExportFilesDestinations destinations)
        {
            throw new NotSupportedException();
        }

        public RavenJObject StripReplicationInformationFromMetadata(RavenJObject metadata)
        {
            if (metadata != null)
            {
                metadata.Remove(SynchronizationConstants.RavenSynchronizationHistory);
                metadata.Remove(SynchronizationConstants.RavenSynchronizationSource);
                metadata.Remove(SynchronizationConstants.RavenSynchronizationVersion);
            }

            return metadata;
        }

        public RavenJObject DisableVersioning(RavenJObject metadata)
        {
            if (metadata != null)
            {
                metadata[Constants.RavenIgnoreVersioning] = true;
            }

            return metadata;
        }
          
        public async Task<Stream> ReceiveFilesInStream(List<string> filePaths)
        {
            if (filePaths == null || filePaths.Count == 0)
            {
                throw new ArgumentException("Should receive file names");
            }
            var asyncFilesCommands = PrimaryStore.AsyncFilesCommands;
            var commands = (AsyncServerClientBase<FilesConvention, IFilesReplicationInformer>) PrimaryStore.AsyncFilesCommands;

            var uri = "/streams/export";
            
            var request = commands.RequestFactory.CreateHttpJsonRequest(
                new CreateHttpJsonRequestParams(PrimaryStore.AsyncFilesCommands, 
                    PrimaryStore.AsyncFilesCommands.UrlFor() + uri, 
                    HttpMethods.Post, 
                    commands.PrimaryCredentials, 
                    commands.Conventions))
                .AddOperationHeaders(commands.OperationsHeaders);
            
            try
            {
                var fileNamesJson = RavenJObject.FromObject(new
                {
                    FileNames = filePaths
                });
                    
                var response = await request.ExecuteRawResponseAsync(fileNamesJson).ConfigureAwait(false);

                return new DisposableStream(await response.GetResponseStreamWithHttpDecompression().ConfigureAwait(false), () =>
                {
                    request.Dispose();
                    response.Dispose();
                });
                
            }
            catch (Exception e)
            {
                throw e.SimplifyException();
            }
        }

        public async Task UploadFilesInStream(FileUploadUnitOfWork[] files)
        {
            var workload = new FilesUploadWorker(files);
            var asyncFilesCommands = PrimaryStore.AsyncFilesCommands;
            var commands = (AsyncServerClientBase<FilesConvention, IFilesReplicationInformer>)PrimaryStore.AsyncFilesCommands;
            var uri = "/streams/Import";


            var createHttpJsonRequestParams = new CreateHttpJsonRequestParams(asyncFilesCommands, PrimaryStore.AsyncFilesCommands.UrlFor()  + uri, HttpMethod.Put, commands.PrimaryCredentials, commands.Conventions, timeout: TimeSpan.FromHours(12))
            {
                DisableRequestCompression = true
            };


            var request = commands.RequestFactory.CreateHttpJsonRequest(createHttpJsonRequestParams).AddOperationHeaders(commands.OperationsHeaders);
            
            using (request.Continue100Scope())
            {

                var response = await request.ExecuteRawRequestAsync(async(stream,t)=>await workload.UploadFiles(stream, t).ConfigureAwait(false)).ConfigureAwait(false);

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
        public bool IsEmbedded => false;
    }


    public class SmugglerBetweenRemoteFilesOperations : SmugglerRemoteFilesOperations
    {
        private readonly Func<FilesStore> secondaryStore;

        protected FilesStore SecondaryStore
        {
            get { return secondaryStore(); }
        }

        public SmugglerBetweenRemoteFilesOperations(Func<FilesStore> primaryStore, Func<FilesStore> secondaryStore, Func<DocumentStore> documentStore) 
            : base ( primaryStore, documentStore )
        {
            if (secondaryStore == null)
                throw new ArgumentNullException("primaryStore");

            this.secondaryStore = secondaryStore;
        }

        public override Task PutFile(FileHeader file, Stream data, long size)
        {
            return SecondaryStore.AsyncFilesCommands.UploadRawAsync(file.FullPath, data, file.Metadata, size);
        }

        public override string CreateIncrementalKey()
        {
            return this.PrimaryStore.AsyncFilesCommands.UrlFor();            
        }

        public override Task<ExportFilesDestinations> GetIncrementalExportKey()
        {
            return this.SecondaryStore.AsyncFilesCommands.Configuration.GetKeyAsync<ExportFilesDestinations>(ExportFilesDestinations.RavenDocumentKey);
        }

        public override Task PutIncrementalExportKey(ExportFilesDestinations destinations)
        {
            return this.SecondaryStore.AsyncFilesCommands.Configuration.SetKeyAsync<ExportFilesDestinations>(ExportFilesDestinations.RavenDocumentKey, destinations);
        }

        public override Task PutConfig(string name, RavenJObject value)
        {
            return SecondaryStore.AsyncFilesCommands.Configuration.SetKeyAsync(name, value);
        }
    }
}
