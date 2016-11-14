using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Abstractions.FileSystem;
using Raven.NewClient.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Extensions;

namespace Raven.NewClient.Client.FileSystem
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


        Task<FileHeader> LoadFileAsync(string path);
        Task<FileHeader[]> LoadFileAsync(IEnumerable<string> path);

        Task<Stream> DownloadAsync(string path, Reference<RavenJObject> metadata = null);
        Task<Stream> DownloadAsync(FileHeader path, Reference<RavenJObject> metadata = null);


        void RegisterUpload(string path, Stream stream, RavenJObject metadata = null, long? etag = null);
        void RegisterUpload(FileHeader path, Stream stream, long? etag = null);
        void RegisterUpload(string path, long fileSize, Action<Stream> write, RavenJObject metadata = null, long? etag = null);
        void RegisterUpload(FileHeader path, long fileSize, Action<Stream> write, long? etag = null);

        void RegisterFileDeletion(string path, long? etag = null);
        void RegisterFileDeletion(FileHeader path, long? etag = null);

        void RegisterDeletionQuery(string query);

        void RegisterRename(string sourceFile, string destinationFile, long? etag = null);
        void RegisterRename(FileHeader sourceFile, string destinationFile, long? etag = null);

        /// <summary>
        /// Saves all the changes to the Raven server.
        /// </summary>
        Task SaveChangesAsync();
    }
}
