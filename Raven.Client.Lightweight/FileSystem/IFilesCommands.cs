using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem
{
    public interface IFilesCommands
    {
        /// <summary>
        /// Gets the operations headers.
        /// </summary>
        /// <value>The operations headers.</value>
        NameValueCollection OperationsHeaders { get; set; }

        /// <summary>
        /// Admin operations for current database
        /// </summary>
        IFilesAdminCommands Admin { get; }

        /// <summary>
        /// Configuration commands used to change the general configuration of file systems.
        /// </summary>
        IFilesConfigurationCommands Configuration { get; }

        /// <summary>
        /// Very low level storage commands.
        /// </summary>
        IFilesStorageCommands Storage { get; }

        /// <summary>
        /// Low level synchronization commands.
        /// </summary>
        IFilesSynchronizationCommands Synchronization { get; }

        /// <summary>
        /// Primary credentials for access. Will be used also in replication context - for failovers
        /// </summary>
        OperationCredentials PrimaryCredentials { get; }

        FilesConvention Conventions { get; }

        string FileSystem { get; }

        void Delete(string filename);
        void Rename(string currentName, string newName);

        RavenJObject GetMetadataFor(string filename);
        void UpdateMetadata(string filename, RavenJObject metadata);

        void Upload(string filename, Stream source);
        void Upload(string filename, RavenJObject metadata, Stream source);
        void Upload(string filename, RavenJObject metadata, Stream source, Action<string, long> progress);

        RavenJObject Download(string filename, Stream destination, long? from = null, long? to = null);

        SearchResults GetFilesFrom(string folder, FilesSortOptions options = FilesSortOptions.Default, string fileNameSearchPattern = "", int start = 0, int pageSize = 25);
        SearchResults Search(string query, string[] sortFields = null, int start = 0, int pageSize = 25);
        string[] GetFolders(string from = null, int start = 0, int pageSize = 25);        

        string[] GetSearchFields(int start = 0, int pageSize = 25);

        FileHeader[] Browse(int start = 0, int pageSize = 25);        
    }

    public interface IFilesAdminCommands
    {
        IFilesCommands Commands { get; }

        string[] GetNames();
        FileSystemStats[] GetStatistics();

        void CreateFileSystem(FileSystemDocument filesystemDocument, string newFileSystemName = null);
        void CreateOrUpdateFileSystem(FileSystemDocument filesystemDocument, string newFileSystemName = null);
        void DeleteFileSystem(string fileSystemName = null, bool hardDelete = false);
    }

    public interface IFilesConfigurationCommands
    {
        IFilesCommands Commands { get; }

        string[] GetKeyNames(int start = 0, int pageSize = 25);

        void SetKey<T>(string key, T data);
        T GetKey<T>(string key);
        void DeleteKey(string key);

        ConfigurationSearchResults Search(string prefix, int start = 0, int pageSize = 25);
    }

    public interface IFilesSynchronizationCommands
    {
        IFilesCommands Commands { get; }

        void SetDestinations(params SynchronizationDestination[] destinations);

        SynchronizationReport GetSynchronizationStatusFor(string filename);
        SourceSynchronizationInformation GetLastSynchronizationFrom(Guid serverId);

        ItemsPage<ConflictItem> GetConflicts(int page = 0, int pageSize = 25);
        void ResolveConflict(string filename, ConflictResolutionStrategy strategy);
        void ApplyConflict(string filename, long remoteVersion, string remoteServerId, IEnumerable<HistoryItem> remoteHistory, string remoteServerUrl);

        SynchronizationConfirmation[] GetConfirmationForFiles(IEnumerable<Tuple<string, Guid>> sentFiles);
        
        ItemsPage<SynchronizationDetails> GetPending(int page = 0, int pageSize = 25);
        ItemsPage<SynchronizationDetails> GetActive(int page = 0, int pageSize = 25);
        ItemsPage<SynchronizationReport> GetFinished(int page = 0, int pageSize = 25);

        void DownloadSignature(string sigName, Stream destination, long? from = null, long? to = null);


        void IncrementLastETag(Guid sourceServerId, string sourceFileSystemUrl, Guid sourceFileETag);

        SignatureManifest GetRdcManifest(string path);
        RdcStats GetRdcStats();


        DestinationSyncResult[] Synchronize(bool forceSyncingAll = false);
        SynchronizationReport Star(string filename, IAsyncFilesCommands destination);
        SynchronizationReport Start(string filename, SynchronizationDestination destination);

        SynchronizationReport Delete(string filename, RavenJObject metadata, ServerInfo sourceServer);
        SynchronizationReport Rename(string filename, string newName, RavenJObject currentMetadata, ServerInfo sourceServer);
        SynchronizationReport UpdateMetadata(string filename, RavenJObject metadata, ServerInfo sourceServer);
    }

    public interface IFilesStorageCommands
    {
        IFilesCommands Commands { get; }

        void CleanUp();
        void RetryRenaming();
    }

}
