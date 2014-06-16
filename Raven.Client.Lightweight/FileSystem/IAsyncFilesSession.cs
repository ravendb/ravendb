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
        Task<FileHeader[]> LoadFileAsync(IEnumerable<string> path);

        Task<FileHeader[]> LoadFilesAtDirectoryAsync(DirectoryHeader directory);
        Task<FileHeader[]> LoadFilesAtDirectoryAsync(string directory);

        Task<DirectoryHeader> LoadDirectoryAsync(string path);

        Task<Stream> DownloadAsync(string path);
        Task<Stream> DownloadAsync(FileHeader path);


        void RegisterUpload(string path, Stream stream, RavenJObject metadata = null, Etag etag = null);
        void RegisterUpload(FileHeader path, Stream stream, RavenJObject metadata = null, Etag etag = null);
        void RegisterUpload(string path, Action<Stream> write, RavenJObject metadata = null, Etag etag = null);
        void RegisterUpload(FileHeader path, Action<Stream> write, RavenJObject metadata = null, Etag etag = null);

        void RegisterFileDeletion(string path, Etag etag = null);
        void RegisterFileDeletion(FileHeader path, Etag etag = null);

        void RegisterDirectoryDeletion(string path, bool recurse = false);
        void RegisterDirectoryDeletion(DirectoryHeader path, bool recurse = false);

        void RegisterRename(string sourceFile, string destinationFile);
        void RegisterRename(FileHeader sourceFile, string destinationFile);
        void RegisterRename(FileHeader sourceFile, DirectoryHeader destinationPath, string destinationName);
        /// <summary>
        /// Saves all the changes to the Raven server.
        /// </summary>
        Task SaveChangesAsync();
    }
}
