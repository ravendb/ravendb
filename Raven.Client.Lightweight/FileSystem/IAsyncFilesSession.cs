using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.Extensions;

namespace Raven.Client.FileSystem
{
    public interface IAsyncFilesSession : IDisposable
    {
        /// <summary>
        /// Get the accessor for advanced operations
        /// </summary>
        /// <remarks>
        /// Those operations are rarely needed, and have been moved to a separate 
        /// property to avoid cluttering the API
        /// </remarks>
        IAsyncAdvancedFilesSessionOperations Advanced { get; }

        IAsyncFilesCommands Commands { get; }

        IAsyncFilesQuery<FileHeader> Query();

        /// <summary>
        /// load a single file in a single call
        /// </summary>
        /// <param name="path">The full file path to load</param>
        /// <returns>The file instance represented by the <see cref="FileHeader"/> object or null if a file does not exist</returns>
        Task<FileHeader> LoadFileAsync(string path);

        /// <summary>
        /// load multiple files in a single call
        /// </summary>
        /// <param name="path">The collection of the file paths to load</param>
        /// <returns>The array of file instances, each represented by <see cref="FileHeader"/> object or null if a file does not exist</returns>
        Task<FileHeader[]> LoadFileAsync(IEnumerable<string> path);

        /// <summary>
        /// Allow you to retrieve the content of a file
        /// </summary>
        /// <param name="path">The full file path</param>
        /// <param name="metadata">Metadata of the downloaded file</param>
        /// <returns>A task that represents the asynchronous download operation</returns>
        Task<Stream> DownloadAsync(string path, Reference<RavenJObject> metadata = null);

        /// <summary>
        /// Allow you to retrieve the content of a file
        /// </summary>
        /// <param name="file">The file represented by the <see cref="FileHeader"/></param>
        /// <param name="metadata">Metadata of the downloaded file</param>
        /// <returns>A task that represents the asynchronous download operation</returns>
        Task<Stream> DownloadAsync(FileHeader file, Reference<RavenJObject> metadata = null);

        /// <summary>
        /// Register an upload by specifying the file's full path and content
        /// </summary>
        /// <param name="path">The full path of the file</param>
        /// <param name="stream">The file content that will be copied to the HTTP request</param>
        /// <param name="metadata">The file's metadata</param>
        /// <param name="etag">Current file Etag, used for concurrency checks (null will skip the check)</param>
        void RegisterUpload(string path, Stream stream, RavenJObject metadata = null, Etag etag = null);

        /// <summary>
        /// Register an upload by specifying the <see cref="FileHeader"/> of the file
        /// </summary>
        /// <param name="file">The file represented by the FileHeader</param>
        /// <param name="stream">The file content that will be copied to the HTTP request</param>
        /// <param name="etag">Current file Etag, used for concurrency checks (null will skip the check)</param>
        void RegisterUpload(FileHeader file, Stream stream, Etag etag = null);

        /// <summary>
        /// Register an upload by specifying the file's full path and content
        /// </summary>
        /// <param name="path">The full path of the file</param>
        /// <param name="fileSize">The declared number of bytes to write in write action</param>
        /// <param name="write">The action which writes file content bytes directly to the HTTP request stream</param>
        /// <param name="metadata">The file's metadata</param>
        /// <param name="etag">Current file Etag, used for concurrency checks (null will skip the check)</param>
        void RegisterUpload(string path, long fileSize, Action<Stream> write, RavenJObject metadata = null, Etag etag = null);

        /// <summary>
        /// Register an upload by specifying the <see cref="FileHeader"/> of the file
        /// </summary>
        /// <param name="file">The file represented by the FileHeader</param>
        /// <param name="fileSize">The declared number of bytes to write in write action</param>
        /// <param name="write">The action which writes file content bytes directly to the HTTP request stream</param>
        /// <param name="etag">Current file Etag, used for concurrency checks (null will skip the check)</param>
        void RegisterUpload(FileHeader file, long fileSize, Action<Stream> write, Etag etag = null);

        /// <summary>
        /// Register a file delete operation
        /// </summary>
        /// <param name="path">The full file path</param>
        /// <param name="etag">Current file Etag, used for concurrency checks (null will skip the check)</param>
        void RegisterFileDeletion(string path, Etag etag = null);

        /// <summary>
        /// Register a file delete operation
        /// </summary>
        /// <param name="file">The file</param>
        /// <param name="etag">Current file Etag, used for concurrency checks (null will skip the check)</param>
        void RegisterFileDeletion(FileHeader file, Etag etag = null);

        /// <summary>
        /// Register deletion of multiple files that match certain criteria
        /// </summary>
        /// <param name="query">The Lucene query</param>
        void RegisterDeletionQuery(string query);

        /// <summary>
        /// Rename a file
        /// </summary>
        /// <param name="sourceFile">The full file path to change</param>
        /// <param name="destinationFile">The new file path</param>
        /// <param name="etag">The current file Etag, used for concurrency checks (null will skip the check)</param>
        void RegisterRename(string sourceFile, string destinationFile, Etag etag = null);

        /// <summary>
        /// Rename a file
        /// </summary>
        /// <param name="sourceFile">The file that you want to rename</param>
        /// <param name="destinationFile"The new file path></param>
        /// <param name="etag">The current file Etag, used for concurrency checks (null will skip the check)</param>
        void RegisterRename(FileHeader sourceFile, string destinationFile, Etag etag = null);

        /// <summary>
        /// Saves all the changes to the Raven server.
        /// </summary>
        Task SaveChangesAsync();
    }
}
