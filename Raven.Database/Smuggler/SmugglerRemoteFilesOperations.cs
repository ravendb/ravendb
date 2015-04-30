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
            return await PrimaryStore.AsyncFilesCommands.Admin.GetStatisticsAsync();
        }

        public virtual async Task<string> GetVersion(FilesConnectionStringOptions server)
        {
            var buildNumber = await DocumentStore.AsyncDatabaseCommands.GlobalAdmin.GetBuildNumberAsync();
            return buildNumber.ProductVersion;
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
            return await PrimaryStore.AsyncFilesCommands.StreamFileHeadersAsync(lastEtag, pageSize: take);
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
		    var names = await PrimaryStore.AsyncFilesCommands.Configuration.GetKeyNamesAsync(start, take);

			var results = new List<KeyValuePair<string, RavenJObject>>(names.Length);

		    foreach (var name in names)
		    {
			    results.Add(new KeyValuePair<string, RavenJObject>(name, await PrimaryStore.AsyncFilesCommands.Configuration.GetKeyAsync<RavenJObject>(name)));
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
			    metadata.Add(Constants.RavenIgnoreVersioning, true);
		    }

		    return metadata;
	    }
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
