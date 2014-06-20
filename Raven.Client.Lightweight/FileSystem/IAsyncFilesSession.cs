using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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


        Task<FileHeader> LoadFileAsync(string path);
        Task<FileHeader> LoadFileAsync(DirectoryHeader directory, string filename);
        Task<FileHeader[]> LoadFilesAsync(IEnumerable<string> path);

        Task<DirectoryHeader> LoadDirectoryAsync(string path);

        Task<Stream> DownloadAsync(string path);
        Task<Stream> DownloadAsync(FileHeader path);


        void RegisterUploadAsync(string path, Stream stream, RavenJObject metadata = null, Etag etag = null);
        void RegisterUploadAsync(FileHeader path, Stream stream, RavenJObject metadata = null, Etag etag = null);
        void RegisterUploadAsync(string path, long start, Action<Stream> write, RavenJObject metadata = null, Etag etag = null);
        void RegisterUploadAsync(FileHeader path, long start, Action<Stream> write, RavenJObject metadata = null, Etag etag = null);

        void RegisterFileDeletionAsync(string path, Etag etag = null);
        void RegisterFileDeletionAsync(FileHeader path, Etag etag = null);

        void RegisterDirectoryDeletionAsync(string path, bool recurse = false);
        void RegisterDirectoryDeletionAsync(DirectoryHeader path, bool recurse = false);

        void RegisterRenameAsync(string sourceFile, string destinationFile);
        void RegisterRenameAsync(FileHeader sourceFile, string destinationFile);
        void RegisterRenameAsync(FileHeader sourceFile, DirectoryHeader destinationPath, string destinationName);
        /// <summary>
        /// Saves all the changes to the Raven server.
        /// </summary>
        Task SaveChangesAsync();
    }
}
