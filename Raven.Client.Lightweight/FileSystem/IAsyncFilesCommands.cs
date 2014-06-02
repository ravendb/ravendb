using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Client.Connection.Profiling;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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


        Task<Guid> GetServerId();

        Task DeleteAsync(string fileName);
        Task RenameAsync(string currentName, string newName);
        Task UpdateMetadataAsync(string filename, RavenJObject metadata);

        Task UploadAsync(string filename, Stream source);
        Task UploadAsync(string filename, RavenJObject metadata, Stream source);
        Task UploadAsync(string filename, RavenJObject metadata, Stream source, Action<string, long> progress);

        Task<RavenJObject> DownloadAsync(string filename, Stream destination, long? from = null, long? to = null);
        
        Task<SearchResults> GetFilesAsync(string folder, FilesSortOptions options = FilesSortOptions.Default, string fileNameSearchPattern = "", int start = 0, int pageSize = 25);
        Task<string[]> GetFoldersAsync(string from = null, int start = 0, int pageSize = 25);        

    }

    public interface IAsyncFilesAdminCommands : IDisposable, IHoldProfilingInformation
    {
        IAsyncFilesCommands Commands { get; }

        Task<string[]> GetFileSystemsNames();
        Task<List<FileSystemStats>> GetFileSystemsStats();
        Task CreateFileSystemAsync(DatabaseDocument databaseDocument, string newFileSystemName = null);
        Task CreateOrUpdateFileSystemAsync(DatabaseDocument databaseDocument, string newFileSystemName = null);
        Task DeleteFileSystemAsync(string fileSystemName = null, bool hardDelete = false);
    }

    public interface IAsyncFilesConfigurationCommands : IDisposable, IHoldProfilingInformation
    {
        IAsyncFilesCommands Commands { get; }

        Task<string[]> GetConfigNames(int start = 0, int pageSize = 25);

        Task SetConfig<T>(string name, T data);
        Task<T> GetConfig<T>(string name);
        Task DeleteConfig(string name);

        Task<ConfigurationSearchResults> SearchAsync(string prefix, int start = 0, int pageSize = 25);
    }

    public interface IAsyncFilesSynchronizationCommands : IDisposable, IHoldProfilingInformation
    {
        IAsyncFilesCommands Commands { get; }

        Task SetDestinationsConfig(params SynchronizationDestination[] destinations);

        Task<RavenJObject> GetMetadataForAsync(string filename);
        
        Task<DestinationSyncResult[]> SynchronizeDestinationsAsync(bool forceSyncingAll = false);        

        Task<SynchronizationReport> GetSynchronizationStatusAsync(string fileName);

        
        Task<ItemsPage<ConflictItem>> GetConflictsAsync(int page = 0, int pageSize = 25);
        Task ResolveConflictAsync(string filename, ConflictResolutionStrategy strategy);
        Task ApplyConflictAsync(string filename, long remoteVersion, string remoteServerId, IList<HistoryItem> remoteHistory, string remoteServerUrl);

        Task<IEnumerable<SynchronizationConfirmation>> ConfirmFilesAsync(IEnumerable<Tuple<string, Guid>> sentFiles);
        Task<ItemsPage<SynchronizationReport>> GetFinishedAsync(int page = 0, int pageSize = 25);
        Task<ItemsPage<SynchronizationDetails>> GetActiveAsync(int page = 0, int pageSize = 25);
        Task<ItemsPage<SynchronizationDetails>> GetPendingAsync(int page = 0, int pageSize = 25);


        Task DownloadSignatureAsync(string sigName, Stream destination, long? from = null, long? to = null);

        Task<SourceSynchronizationInformation> GetLastSynchronizationFromAsync(Guid serverId);
        Task IncrementLastETagAsync(Guid sourceServerId, string sourceFileSystemUrl, Guid sourceFileETag);

        Task<SignatureManifest> GetRdcManifestAsync(string path);
        Task<RdcStats> GetRdcStatsAsync();

        Task<SynchronizationReport> StartAsync(string fileName, IAsyncFilesCommands destination);
        Task<SynchronizationReport> StartAsync(string fileName, SynchronizationDestination destination);

        Task<SynchronizationReport> DeleteAsync(string fileName, RavenJObject metadata, ServerInfo sourceServer);
        Task<SynchronizationReport> RenameAsync(string currentName, string newName, RavenJObject currentMetadata, ServerInfo sourceServer);
        Task<SynchronizationReport> UpdateMetadataAsync(string fileName, RavenJObject metadata, ServerInfo sourceServer);

    }

    public interface IAsyncFilesStorageCommands : IDisposable, IHoldProfilingInformation
    {
        IAsyncFilesCommands Commands { get; }

        Task CleanUp();
        Task RetryRenaming();
    }
}
