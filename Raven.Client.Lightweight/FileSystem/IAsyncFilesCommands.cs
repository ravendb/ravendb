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
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Connection;
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
        /// Admin operations
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

        string FileSystemName { get; }

        string UrlFor(string fileSystem = null);

        IAsyncFilesCommands ForFileSystem(string fileSystemName);
        IAsyncFilesCommands With(ICredentials credentials);
        IAsyncFilesCommands With(OperationCredentials credentials);

        Task<Guid> GetServerIdAsync();
        Task<FileSystemStats> GetStatisticsAsync();

        /// <summary>
        /// Delete a file
        /// </summary>
        /// <param name="filename">The name of a file to be deleted</param>
        /// <param name="etag">The current file Etag, used for concurrency checks (null skips check)</param>
        /// <returns>A task that represents the asynchronous delete operation</returns>
        Task DeleteAsync(string filename, Etag etag = null);

        /// <summary>
        /// Change the file name
        /// </summary>
        /// <param name="currentName">The name of the file that you want to change</param>
        /// <param name="newName">The new name of a file</param>
        /// <param name="etag">	The current file etag used for concurrency checks (null skips check)</param>
        /// <returns>A task that represents the asynchronous rename operation</returns>
        Task RenameAsync(string currentName, string newName, Etag etag = null);
        /// <summary>
        /// Copy a file (server side operation)
        /// </summary>
        /// <param name="sourceName">The name of the file that you want to copy from</param>
        /// <param name="targetNAme">The name of the new file you want to copy to</param>
        /// <param name="etag">The current file etag used for concurrency checks (null skips check)</param>
        /// <returns>A task that represents the asynchronous copy operation</returns>
        Task CopyAsync(string sourceName, string targetNAme, Etag etag = null);

        /// <summary>
        /// To retrieve the file's metadata
        /// </summary>
        /// <param name="fileName">The name of a file</param>
        /// <returns>A task that represents the asynchronous metadata download operation</returns>
        Task<RavenJObject> GetMetadataForAsync(string filename);

        /// <summary>
        /// Change just the file's metadata without any modification to its content
        /// </summary>
        /// <param name="filename">The modified file name</param>
        /// <param name="metadata">New file metadata</param>
        /// <param name="etag">The current file Etag, used for concurrency checks (null skips check</param>
        /// <returns>A task that represents the asynchronous metadata update operation</returns>
        Task UpdateMetadataAsync(string filename, RavenJObject metadata, Etag etag = null);

        /// <summary>
        /// To insert a new file or update the content of an existing one
        /// </summary>
        /// <param name="filename">The name of the uploaded file (full path)</param>
        /// <param name="source">The file content</param>
        /// <param name="metadata">The file metadata (default: null)</param>
        /// <param name="etag">The current file etag used for concurrency checks (null skips check)</param>
        /// <returns>A task that represents the asynchronous upload operation</returns>
        Task UploadAsync(string filename, Stream source, RavenJObject metadata = null, Etag etag = null);

        /// <summary>
        /// To insert a new file or update the content of an existing one
        /// </summary>
        /// <param name="filename">The name of the uploaded file (full path)</param>
        /// <param name="source">The file content</param>
        /// <param name="prepareStream">The action executed before the content is being written (null means no action to perform)</param>
        /// <param name="size">The file size It is sent in RavenFS-Size header to validate the number of bytes received on the server side.</param>
        /// <param name="metadata">The file metadata (default: null)</param>
        /// <param name="etag">The current file etag used for concurrency checks (null skips check)</param>
        /// <returns>A task that represents the asynchronous upload operation</returns>
        Task UploadAsync(string filename, Action<Stream> source, Action prepareStream, long? size, RavenJObject metadata = null, Etag etag = null);

        Task UploadRawAsync(string filename, Stream source, RavenJObject metadata, long? size, Etag etag = null);

        /// <summary>
        /// Retrieve the file's content and metadata
        /// </summary>
        /// <param name="filename">The name of a downloaded file</param>
        /// <param name="metadata">Reference of metadata object where downloaded file metadata will be placed</param>
        /// <param name="from">The number of the first byte in a range when a partial download is requested</param>
        /// <param name="to">The number of the last byte in a range when a partial download is requested</param>
        /// <returns>A task that represents the asynchronous download operation</returns>
        Task<Stream> DownloadAsync(string filename, Reference<RavenJObject> metadata = null, long? from = null, long? to = null);

        /// <summary>
        /// Retrieve the paths of subdirectories of a specified directory
        /// </summary>
        /// <param name="from">The directory path (default: null means the root directory)</param>
        /// <param name="start">The number of results that should be skipped (for paging purposes)</param>
        /// <param name="pageSize">The max number of results to get</param>
        /// <returns>A task that represents the asynchronous operation</returns>
        Task<string[]> GetDirectoriesAsync(string from = null, int start = 0, int pageSize = 1024);

        /// <summary>
        /// Retrieve the list of all available field names to build a query
        /// </summary>
        /// <param name="start">The number of results that should be skipped</param>
        /// <param name="pageSize">The maximum number of results that will be returned</param>
        /// <returns>A task that represents the asynchronous operation. The task result is the array of indexed field names</returns>
        Task<string[]> GetSearchFieldsAsync(int start = 0, int pageSize = 1024);

        /// <summary>
        /// Fetch the list of files matching the specified query
        /// </summary>
        /// <param name="query">The query containing search criteria (you can use the built-in fields or metadata entries) consistent with Lucene syntax</param>
        /// <param name="sortFields">The fields to sort by</param>
        /// <param name="start">The start number to read index results</param>
        /// <param name="pageSize">The max number of results that will be returned</param>
        /// <returns>A task that represents the asynchronous operation</returns>
        Task<SearchResults> SearchAsync(string query, string[] sortFields = null, int start = 0, int pageSize = 1024);

        /// <summary>
        /// Delete files that match the specified query.
        /// </summary>
        /// <param name="query">The Lucene query</param>
        /// <returns>A task that represents the asynchronous delete operation</returns>
        Task DeleteByQueryAsync(string query);

        /// <summary>
        /// Returns files located in a given directory and matching specified file name search pattern
        /// </summary>
        /// <param name="folder">The directory path to look for files</param>
        /// <param name="options">It determines the sorting options when returning results</param>
        /// <param name="fileNameSearchPattern">The pattern that a file name has to match ('?' any single character, '*' any characters, default: empty string - means that a matching file name is skipped)</param>
        /// <param name="start">The number of files that should be skipped</param>
        /// <param name="pageSize">The maximum number of files that will be returned</param>
        /// <returns>A task that represents the asynchronous operation</returns>
        Task<SearchResults> SearchOnDirectoryAsync(string folder, FilesSortOptions options = FilesSortOptions.Default, string fileNameSearchPattern = "", int start = 0, int pageSize = 1024);

        /// <summary>
        /// Scan a file system for existing files
        /// </summary>
        /// <param name="start">The number of files that will be skipped</param>
        /// <param name="pageSize">The maximum number of file headers that will be retrieved (default: 1024)</param>
        /// <returns>A task that represents the asynchronous browse operation.</returns>
        Task<FileHeader[]> BrowseAsync(int start = 0, int pageSize = 1024);


        Task<TouchFilesResult> TouchFilesAsync(Etag start, int pageSize);

        /// <summary>
        /// Get the file headers of the selected files
        /// </summary>
        /// <param name="filename">Names of the files you want to get headers for</param>
        /// <returns>A task that represents the asynchronous get operation</returns>
        Task<FileHeader[]> GetAsync(string[] filename);

        /// <summary>
        /// Retrieve multiple file headers for the specified prefix name
        /// </summary>
        /// <param name="prefix">The prefix that the returned files need to match</param>
        /// <param name="matches">Pipe ('|') separated values for which file name (after 'prefix') should be matched ('?' any single character; '*' any characters)</param>
        /// <param name="start">The number of files that should be skipped</param>
        /// <param name="pageSize">The maximum number of the file headers that will be returned</param>
        /// <returns>A task that represents the asynchronous operation</returns>
        Task<FileHeader[]> StartsWithAsync(string prefix, string matches, int start, int pageSize);

        /// <summary>
        /// Stream the headers of files which match the criteria chosen from a file system
        /// </summary>
        /// <param name="fromEtag">ETag of a file from which the stream should start</param>
        /// <param name="pageSize">The maximum number of file headers that will be retrieved</param>
        /// <returns>A task that represents the asynchronous operation</returns>
        Task<IAsyncEnumerator<FileHeader>> StreamFileHeadersAsync(Etag fromEtag, int pageSize = int.MaxValue);
        /// <summary>
        /// Stream the query results to the client
        /// </summary>
        /// <param name="query">The Lucene query</param>
        /// <param name="sortFields">The fields to sort by</param>
        /// <param name="start">The number of files that should be skipped</param>
        /// <param name="pageSize">The maximum number of file headers that will be retrieved</param>
        /// <returns>A task that represents the asynchronous operation</returns>
        Task<IAsyncEnumerator<FileHeader>> StreamQueryAsync(string query, string[] sortFields = null, int start = 0, int pageSize = int.MaxValue);
        IDisposable ForceReadFromMaster();
    }

    public interface IAsyncFilesAdminCommands : IDisposable, IHoldProfilingInformation
    {
        IAsyncFilesCommands Commands { get; }

        /// <summary>
        /// Returns the names of all existing file systems in the server
        /// </summary>
        Task<string[]> GetNamesAsync();

        /// <summary>
        /// Returns statistics of currently loaded file systems
        /// </summary>
        Task<FileSystemStats[]> GetStatisticsAsync();

        /// <summary>
        /// Creates a new file system. If the file system already exists then it will throw an exception
        /// </summary>
        /// <param name="filesystemDocument">The document containing all configuration options for a new file system (e.g. active bundles, name/id, data path)</param>
        /// <returns>A task that represents the asynchronous operation</returns>
        Task CreateFileSystemAsync(FileSystemDocument filesystemDocument);

        /// <summary>
        /// Creates a new file system or updates the configuration of already existing one according to the specified document
        /// </summary>
        /// <param name="filesystemDocument">The document containing all configuration options for a new file system (e.g. active bundles, name/id, data path)</param>
        /// <param name="newFileSystemName">The new file system name, if null then current file system name will be used</param>
        /// <returns></returns>
        Task CreateOrUpdateFileSystemAsync(FileSystemDocument filesystemDocument, string newFileSystemName = null);

        /// <summary>
        /// Delete a file system from a server, with a possibility to remove its all data from the hard drive
        /// </summary>
        /// <param name="fileSystemName">The name of a file system to delete, if null then current file system name will be used</param>
        /// <param name="hardDelete">Determines if all data should be removed (data files, indexing files, etc.). Default: false</param>
        /// <returns>A task that represents the asynchronous delete operation</returns>
        Task DeleteFileSystemAsync(string fileSystemName = null, bool hardDelete = false);

        /// <summary>
        /// Make sure that file system exists. If there is no such file system, it will be created with default settings
        /// </summary>
        /// <param name="fileSystem">The file system name</param>
        /// <returns>A task that represents the asynchronous operation</returns>
        Task EnsureFileSystemExistsAsync(string fileSystem);

        /// <summary>
        /// Restore backup
        /// </summary>
        /// <param name="restoreRequest">Restore information</param>
        /// <returns>A task that represents the asynchronous restore operation. The task result is the operation identifier</returns>
        Task<long> StartRestore(FilesystemRestoreRequest restoreRequest);

        /// <summary>
        /// Start backup (WARNING: This operation makes the file system offline for the time of compaction)
        /// </summary>
        /// <param name="backupLocation">The path to a directory where the backup will be stored</param>
        /// <param name="fileSystemDocument">The file system configuration document (WARNING: The file system configuration document may contain sensitive data which will be decrypted and stored in the backup)</param>
        /// <param name="incremental">Indicates if it should be the incremental backup</param>
        /// <param name="fileSystemName">The name of the file system to backup</param>
        /// <returns>A task that represents the asynchronous start operation</returns>
        Task<long> StartBackup(string backupLocation, FileSystemDocument fileSystemDocument, bool incremental, string fileSystemName);

        /// <summary>
        /// Initializes the compaction of the indicated file system
        /// </summary>
        /// <param name="filesystemName">The name of a file system to compact</param>
        /// <returns>A task that represents the asynchronous restore operation</returns>
        Task<long> StartCompact(string filesystemName);

        /// <summary>
        /// Forces RavenFS to rebuild Lucene indexes from scratch
        /// </summary>
        /// <param name="filesystemName">The name of the file system</param>
        /// <returns>A task that represents the asynchronous restore operation</returns>
        Task ResetIndexes(string filesystemName);
    }

    public interface IAsyncFilesConfigurationCommands : IDisposable, IHoldProfilingInformation
    {
        IAsyncFilesCommands Commands { get; }

        /// <summary>
        /// Retrieves names of all stored configurations
        /// </summary>
        /// <param name="start">The number of results that should be skipped</param>
        /// <param name="pageSize">The maximum number of results that will be returned</param>
        /// <returns>A task that represents the asynchronous operation. The result is the array of configuration names</returns>
        Task<string[]> GetKeyNamesAsync(int start = 0, int pageSize = 25);

        /// <summary>
        /// Store any object as a <see cref="Configuration"/> item under the specified key
        /// </summary>
        /// <param name="key">The configuration name</param>
        /// <param name="data">The stored object that will be serialized to JSON and saved as a configuration</param>
        /// <returns></returns>
        Task SetKeyAsync<T>(string key, T data);

        /// <summary>
        /// Retrieve an object stored as a <see cref="Configuration"/> item in RavenFS
        /// </summary>
        /// <param name="key">The configuration name</param>
        /// <returns>A task that represents the asynchronous operation. The task result is the deserialized object of type T</returns>
        Task<T> GetKeyAsync<T>(string key);

        /// <summary>
        /// Remove a configuration stored under the specified key
        /// </summary>
        /// <param name="key">	The configuration name</param>
        /// <returns>A task that represents the asynchronous remove operation</returns>
        Task DeleteKeyAsync(string key);

        /// <summary>
        /// Retrieves the names of configurations that starts with a specified prefix
        /// </summary>
        /// <param name="prefix">The prefix value with which the name of a configuration has to start</param>
        /// <param name="start">The number of results that should be skipped</param>
        /// <param name="pageSize">The maximum number of results that will be returned</param>
        /// <returns>A task that represents the asynchronous operation. The task result is <see cref="ConfigurationSearchResults"/> object which represents results of a prefix query</returns>
        Task<ConfigurationSearchResults> SearchAsync(string prefix, int start = 0, int pageSize = 25);
    }

    public interface IAsyncFilesSynchronizationCommands : ISynchronizationServerClient, IDisposable, IHoldProfilingInformation
    {
        IAsyncFilesCommands Commands { get; }

        /// <summary>
        /// Retrieve all the configured synchronization destinations
        /// </summary>
        /// <returns>The array of synchronization destinations</returns>
        Task<SynchronizationDestination[]> GetDestinationsAsync();

        /// <summary>
        /// Setup the servers where files should be synchronized
        /// </summary>
        /// <param name="destinations">The array of SynchronizationDestination objects representing destination file systems</param>
        /// <returns>A task that represents the asynchronous set operation</returns>
        Task SetDestinationsAsync(params SynchronizationDestination[] destinations);

        /// <summary>
        /// Retrieves the existing conflict items
        /// </summary>
        /// <param name="start">The number of items to skip</param>
        /// <param name="pageSize">The maximum number of items to get</param>
        /// <returns>A task that represents the asynchronous get operation</returns>
        Task<ItemsPage<ConflictItem>> GetConflictsAsync(int start = 0, int pageSize = 25);

        /// <summary>
        /// Returns a report that contains the information about the synchronization of a specified file
        /// </summary>
        /// <param name="filename">The full file name</param>
        /// <returns>A task that represents the asynchronous get operation. The task result is an <see cref="SynchronizationReport"/></returns>
        Task<SynchronizationReport> GetSynchronizationStatusForAsync(string filename);

        /// <summary>
        /// Page through the <see cref="SynchronizationReport"/> of already accomplished file synchronizations on the destination server
        /// </summary>
        /// <param name="start">The number of reports to skip</param>
        /// <param name="pageSize">The maximum number of reports that will be returned</param>
        /// <returns>A task that represents the asynchronous operation</returns>
        Task<ItemsPage<SynchronizationReport>> GetFinishedAsync(int start = 0, int pageSize = 25);

        /// <summary>
        /// Returns the information about the active outgoing synchronizations
        /// </summary>
        /// <param name="start">The number of items to skip</param>
        /// <param name="pageSize">The maximum number of items to get</param>
        /// <returns>A task that represents the asynchronous operation</returns>
        Task<ItemsPage<SynchronizationDetails>> GetActiveAsync(int start = 0, int pageSize = 25);

        /// <summary>
        /// Returns the information about the files that wait for a synchronization slot to a destination file system
        /// </summary>
        /// <param name="start">The number of items to skip</param>
        /// <param name="pageSize">The maximum number of items to get</param>
        /// <returns>A task that represents the asynchronous operation</returns>
        Task<ItemsPage<SynchronizationDetails>> GetPendingAsync(int start = 0, int pageSize = 25);

        /// <summary>
        /// Manually force the synchronization to the destinations
        /// </summary>
        /// <param name="forceSyncingAll">Determines whether finished synchronization should schedule a next pending one</param>
        /// <returns>A task that represents the asynchronous synchronization operation.The reports of such synchronizations will not be included in <see cref="DestinationSyncResult"/> object</returns>
        Task<DestinationSyncResult[]> StartAsync(bool forceSyncingAll = false);
        Task<SynchronizationReport> StartAsync(string filename, IAsyncFilesCommands destination);

        /// <summary>
        /// Manually force the synchronization to the destinations
        /// </summary>
        /// <param name="filename">The full file name</param>
        /// <param name="destination">The destination file system</param>
        /// <returns>A task that represents the asynchronous file synchronization operation. The task result is a <see cref="SynchronizationReport"/></returns>
        Task<SynchronizationReport> StartAsync(string filename, SynchronizationDestination destination);
    }

    public interface ISynchronizationServerClient : IHoldProfilingInformation
    {
        string BaseUrl { get; }
        FilesConvention Conventions { get; }
        OperationCredentials Credentials { get; }
        HttpJsonRequestFactory RequestFactory { get; }
        Task<RavenJObject> GetMetadataForAsync(string fileName);
        Task DownloadSignatureAsync(string sigName, Stream destination, long? from = null, long? to = null);

        Task<RdcStats> GetRdcStatsAsync();
        Task<SynchronizationReport> RenameAsync(string currentName, string newName, RavenJObject metadata, FileSystemInfo sourceFileSystem);
        Task<SynchronizationReport> DeleteAsync(string fileName, RavenJObject metadata, FileSystemInfo sourceFileSystem);
        Task<SynchronizationReport> UpdateMetadataAsync(string fileName, RavenJObject metadata, FileSystemInfo sourceFileSystem);
        Task<SourceSynchronizationInformation> GetLastSynchronizationFromAsync(Guid serverId);

        /// <summary>
        /// Resolves the conflict according to the specified conflict resolution strategy
        /// </summary>
        /// <param name="filename">The file path</param>
        /// <param name="strategy">The strategy - CurrentVersion or RemoteVersion</param>
        /// <returns>A task that represents the asynchronous resolve operation</returns>
        Task ResolveConflictAsync(string filename, ConflictResolutionStrategy strategy);

        /// <summary>
        /// Resolves all the conflicts according to the specified conflict resolution strategy
        /// </summary>
        /// <param name="strategy">The strategy - CurrentVersion or RemoteVersion</param>
        /// <returns></returns>
        Task ResolveConflictsAsync(ConflictResolutionStrategy strategy);
        Task ApplyConflictAsync(string filename, long remoteVersion, string remoteServerId, RavenJObject remoteMetadata, string remoteServerUrl);
        Task<ConflictResolutionStrategy> GetResolutionStrategyFromDestinationResolvers(ConflictItem conflict, RavenJObject localMetadata);
        Task<SynchronizationConfirmation[]> GetConfirmationForFilesAsync(IEnumerable<Tuple<string, Etag>> sentFiles);
        Task<SignatureManifest> GetRdcManifestAsync(string path);
        Task IncrementLastETagAsync(Guid sourceServerId, string sourceFileSystemUrl, Etag sourceFileETag, bool force = false);
    }

    public interface IAsyncFilesStorageCommands : IDisposable, IHoldProfilingInformation
    {
        IAsyncFilesCommands Commands { get; }

        /// <summary>
        /// Forces to run a background task that will clean up files marked as deleted
        /// </summary>
        /// <returns>A task that represents the asynchronous operation</returns>
        Task CleanUpAsync();

        /// <summary>
        /// Runs a background task that will resume unaccomplished file renames
        /// </summary>
        /// <returns>A task that represents the asynchronous operation</returns>
        Task RetryRenamingAsync();
        /// <summary>
        /// Runs a background task that will resume unaccomplished file copies
        /// </summary>
        /// <returns>A task that represents the asynchronous operation</returns>
        Task RetryCopyingAsync();
    }
}
