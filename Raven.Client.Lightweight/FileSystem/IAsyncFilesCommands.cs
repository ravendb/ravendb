using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Util;
using Raven.Client.Connection.Profiling;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using FileSystemInfo = Raven.Abstractions.FileSystem.FileSystemInfo;

namespace Raven.Client.FileSystem
{
    public interface IAsyncFilesCommands : IDisposable, IHoldProfilingInformation
    {
        /// <summary>
        /// Gets the operations headers.
        /// </summary>
        /// <value>The operations headers.</value>
        NameValueCollection OperationsHeaders { get; set; }

        /// <summary>
        /// Admin operations for current database
        /// </summary>
        IAsyncFilesAdminCommands Admin { get; }

        /// <summary>
        /// Configuration commands used to change the general configuration of file systems.
        /// </summary>
        IAsyncFilesConfigurationCommands Configuration { get; }

        /// <summary>
        /// Very low level storage commands.
        /// </summary>
        IAsyncFilesStorageCommands Storage { get; }

        /// <summary>
        /// Low level synchronization commands.
        /// </summary>
        IAsyncFilesSynchronizationCommands Synchronization { get; }

        /// <summary>
        /// Primary credentials for access. Will be used also in replication context - for failovers
        /// </summary>
        OperationCredentials PrimaryCredentials { get; }

        FilesConvention Conventions { get; }

        string FileSystem { get; }

        string UrlFor(string fileSystem = null);

        IAsyncFilesCommands ForFileSystem(string fileSystem);
        IAsyncFilesCommands With(ICredentials credentials);
        IAsyncFilesCommands With(OperationCredentials credentials);

        Task<Guid> GetServerIdAsync();
        Task<FileSystemStats> GetStatisticsAsync();

        Task DeleteAsync(string filename, Etag etag = null);
        Task RenameAsync(string currentName, string newName, Etag etag = null);
	    
        Task<RavenJObject> GetMetadataForAsync(string filename);

        Task UpdateMetadataAsync(string filename, RavenJObject metadata, Etag etag = null);

        Task UploadAsync(string filename, Stream source, RavenJObject metadata = null, long? size = null, Etag etag = null);
        Task UploadRawAsync(string filename, Stream source, RavenJObject metadata, long size, Etag etag = null);

        Task<Stream> DownloadAsync(string filename, Reference<RavenJObject> metadata = null, long? from = null, long? to = null);

        Task<string[]> GetDirectoriesAsync(string from = null, int start = 0, int pageSize = 1024);

        Task<string[]> GetSearchFieldsAsync(int start = 0, int pageSize = 1024);
        Task<SearchResults> SearchAsync(string query, string[] sortFields = null, int start = 0, int pageSize = 1024);
		Task DeleteByQueryAsync(string query, string[] orderByFields = null, int start = 0, int pageSize = int.MaxValue);
        Task<SearchResults> SearchOnDirectoryAsync(string folder, FilesSortOptions options = FilesSortOptions.Default, string fileNameSearchPattern = "", int start = 0, int pageSize = 1024);
	    
        Task<FileHeader[]> BrowseAsync(int start = 0, int pageSize = 1024);

        Task<FileHeader[]> GetAsync(string[] filename);
		Task<FileHeader[]> StartsWithAsync(string prefix, string matches, int start, int pageSize);

        Task<IAsyncEnumerator<FileHeader>> StreamFileHeadersAsync(Etag fromEtag, int pageSize = int.MaxValue);
    }

    public interface IAsyncFilesAdminCommands : IDisposable, IHoldProfilingInformation
    {
        IAsyncFilesCommands Commands { get; }

        Task<string[]> GetNamesAsync();
        Task<FileSystemStats[]> GetStatisticsAsync();

        Task CreateFileSystemAsync(FileSystemDocument filesystemDocument, string newFileSystemName = null);
        Task CreateOrUpdateFileSystemAsync(FileSystemDocument filesystemDocument, string newFileSystemName = null);
        Task DeleteFileSystemAsync(string fileSystemName = null, bool hardDelete = false);

        Task EnsureFileSystemExistsAsync(string fileSystem);        
        Task<long> StartRestore(FilesystemRestoreRequest restoreRequest);
        Task StartBackup(string backupLocation, FileSystemDocument databaseDocument, bool incremental, string filesystemName);
        Task<long> StartCompact(string filesystemName);
        Task ResetIndexes(string filesystemName);
    }

    public interface IAsyncFilesConfigurationCommands : IDisposable, IHoldProfilingInformation
    {
        IAsyncFilesCommands Commands { get; }

        Task<string[]> GetKeyNamesAsync(int start = 0, int pageSize = 25);

        Task SetKeyAsync<T>(string key, T data);
        Task<T> GetKeyAsync<T>(string key);
        Task DeleteKeyAsync(string key);

        Task<ConfigurationSearchResults> SearchAsync(string prefix, int start = 0, int pageSize = 25);
    }

    public interface IAsyncFilesSynchronizationCommands : IDisposable, IHoldProfilingInformation
    {
        IAsyncFilesCommands Commands { get; }

        Task<SynchronizationDestination[]> GetDestinationsAsync();
        Task SetDestinationsAsync(params SynchronizationDestination[] destinations);

        Task<SynchronizationReport> GetSynchronizationStatusForAsync(string filename);
        Task<SourceSynchronizationInformation> GetLastSynchronizationFromAsync(Guid serverId);
        
        Task<ItemsPage<ConflictItem>> GetConflictsAsync(int page = 0, int pageSize = 25);
        Task ResolveConflictAsync(string filename, ConflictResolutionStrategy strategy);
        Task ApplyConflictAsync(string filename, long remoteVersion, string remoteServerId, RavenJObject remoteMetadata, string remoteServerUrl);
		Task<ConflictResolutionStrategy> GetResolutionStrategyFromDestinationResolvers(ConflictItem conflict, RavenJObject localMetadata);

        Task<SynchronizationConfirmation[]> GetConfirmationForFilesAsync(IEnumerable<Tuple<string, Etag>> sentFiles);

        Task<ItemsPage<SynchronizationReport>> GetFinishedAsync(int page = 0, int pageSize = 25);
        Task<ItemsPage<SynchronizationDetails>> GetActiveAsync(int page = 0, int pageSize = 25);
        Task<ItemsPage<SynchronizationDetails>> GetPendingAsync(int page = 0, int pageSize = 25);


        Task DownloadSignatureAsync(string sigName, Stream destination, long? from = null, long? to = null);

        
        Task IncrementLastETagAsync(Guid sourceServerId, string sourceFileSystemUrl, Etag sourceFileETag);

        Task<SignatureManifest> GetRdcManifestAsync(string path);
        Task<RdcStats> GetRdcStatsAsync();


        Task<DestinationSyncResult[]> SynchronizeAsync(bool forceSyncingAll = false);
        Task<SynchronizationReport> StartAsync(string filename, IAsyncFilesCommands destination);
        Task<SynchronizationReport> StartAsync(string filename, SynchronizationDestination destination);

        Task<SynchronizationReport> DeleteAsync(string filename, RavenJObject metadata, FileSystemInfo sourceFileSystem);
        Task<SynchronizationReport> RenameAsync(string filename, string newName, RavenJObject metadata, FileSystemInfo sourceFileSystem);
        Task<SynchronizationReport> UpdateMetadataAsync(string filename, RavenJObject metadata, FileSystemInfo sourceFileSystem);
    }

    public interface IAsyncFilesStorageCommands : IDisposable, IHoldProfilingInformation
    {
        IAsyncFilesCommands Commands { get; }

        Task CleanUpAsync();
        Task RetryRenamingAsync();
    }
}
