using Raven.Abstractions.Data;
using Raven.Abstractions.RavenFS;
using Raven.Client.Connection.Profiling;
using Raven.Client.RavenFS;
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


        Task<Guid> GetServerId();

        Task<RavenJObject> DownloadAsync(string filename, Stream destination, long? from = null, long? to = null);

        Task UpdateMetadataAsync(string filename, RavenJObject metadata);

        Task UploadAsync(string filename, Stream source);
        Task UploadAsync(string filename, RavenJObject metadata, Stream source);
        Task UploadAsync(string filename, RavenJObject metadata, Stream source, Action<string, long> progress);


        Task<SynchronizationReport> DeleteAsync(string fileName, RavenJObject metadata, ServerInfo sourceServer);
        Task<SynchronizationReport> RenameAsync(string currentName, string newName, RavenJObject currentMetadata, ServerInfo sourceServer);
        Task<SynchronizationReport> UpdateMetadataAsync(string fileName, RavenJObject metadata, ServerInfo sourceServer);

        
        Task<SearchResults> GetFilesAsync(string folder, FilesSortOptions options = FilesSortOptions.Default, string fileNameSearchPattern = "", int start = 0, int pageSize = 25);
        Task<string[]> GetFoldersAsync(string from = null, int start = 0, int pageSize = 25);        

    }

    public interface IAsyncFilesAdminCommands : IDisposable, IHoldProfilingInformation
    {
        Task<string[]> GetFileSystemsNames();
        Task<List<FileSystemStats>> GetFileSystemsStats();
        Task CreateFileSystemAsync(DatabaseDocument databaseDocument, string newFileSystemName = null);
        Task CreateOrUpdateFileSystemAsync(DatabaseDocument databaseDocument, string newFileSystemName = null);
        Task DeleteFileSystemAsync(string fileSystemName = null, bool hardDelete = false);
    }

    public interface IAsyncFilesConfigurationCommands : IDisposable, IHoldProfilingInformation
    {
        Task<string[]> GetConfigNames(int start = 0, int pageSize = 25);

        Task SetConfig<T>(string name, T data);
        Task<T> GetConfig<T>(string name);
        Task DeleteConfig(string name);

        Task<ConfigSearchResults> SearchAsync(string prefix, int start = 0, int pageSize = 25);
    }

    public interface IAsyncFilesSynchronizationCommands : IDisposable, IHoldProfilingInformation
    {
        Task SetDestinationsConfig(params SynchronizationDestination[] destinations);

        Task<RavenJObject> GetMetadataForAsync(string filename);
        
        Task<DestinationSyncResult[]> SynchronizeDestinationsAsync(bool forceSyncingAll = false);

        Task<SynchronizationReport> StartAsync(string fileName);
        Task<SynchronizationReport> StartAsync(string fileName, SynchronizationDestination destination);


        Task<SynchronizationReport> GetSynchronizationStatusAsync(string fileName);

        
        Task<ListPage<ConflictItem>> GetConflictsAsync(int page = 0, int pageSize = 25);
        Task ResolveConflictAsync(string filename, ConflictResolutionStrategy strategy);
        Task ApplyConflictAsync(string filename, long remoteVersion, string remoteServerId, IList<HistoryItem> remoteHistory, string remoteServerUrl);
        
                
        Task<ListPage<SynchronizationReport>> GetFinishedAsync(int page = 0, int pageSize = 25);
        Task<ListPage<SynchronizationDetails>> GetActiveAsync(int page = 0, int pageSize = 25);
        Task<ListPage<SynchronizationDetails>> GetPendingAsync(int page = 0, int pageSize = 25);        
        
    }

    public interface IAsyncFilesStorageCommands : IDisposable, IHoldProfilingInformation
    {
        Task CleanUp();
        Task RetryRenaming();
    }
}
