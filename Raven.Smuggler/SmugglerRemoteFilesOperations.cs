using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.FileSystem;
using Raven.Json.Linq;

namespace Raven.Smuggler
{
    public class SmugglerRemoteFilesOperations : ISmugglerFilesOperations
    {
        private readonly Func<FilesStore> store;
        private readonly Func<DocumentStore> documentStore;

        const int RetriesCount = 5;

        private FilesStore Store
        {
            get { return store(); }
        }

        private DocumentStore DocumentStore
        {
            get { return documentStore(); }
        }        


        public SmugglerFilesOptions Options { get; private set; }

        public bool LastRequestErrored { get; set; }

        public SmugglerRemoteFilesOperations(Func<FilesStore> store, Func<DocumentStore> documentStore)
        {
            this.store = store;
            this.documentStore = documentStore;
        }

        public async Task<FileSystemStats[]> GetStats()
        {
            return await Store.AsyncFilesCommands.Admin.GetStatisticsAsync();
        }

        public async Task<string> GetVersion(FilesConnectionStringOptions server)
        {
            var buildNumber = await DocumentStore.AsyncDatabaseCommands.GlobalAdmin.GetBuildNumberAsync();
            return buildNumber.ProductVersion;
        }

        public LastFilesEtagsInfo FetchCurrentMaxEtags()
        {
            return new LastFilesEtagsInfo
            { 
                LastFileEtag = null,
                LastDeletedFileEtag = null
            };
        }

        public async Task<IAsyncEnumerator<FileHeader>> GetFiles(FilesConnectionStringOptions src, Etag lastEtag, int take)
        {
            ShowProgress("Streaming documents from {0}, batch size {1}", lastEtag, take);
            return await Store.AsyncFilesCommands.StreamFilesAsync(lastEtag, pageSize: take);
        }

        public Task<Stream> DownloadFile(FileHeader file)
        {
            return Store.AsyncFilesCommands.DownloadAsync(file.FullPath);
        }

        public Task PutFiles(Stream files, RavenJObject metadata)
        {
            throw new NotImplementedException();
        }

        public void Initialize(SmugglerFilesOptions options)
        {
            this.Options = options;
        }

        public void Configure(SmugglerFilesOptions options)
        {
            if (Store.HasJsonRequestFactory == false)
                return;
        }

        public void ShowProgress(string format, params object[] args)
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
    }
}
