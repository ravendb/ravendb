using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
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


        Task<FileHeader> LoadFileAsync(string path);
        Task<FileHeader[]> LoadFileAsync(IEnumerable<string> path);

        Task<Stream> DownloadAsync(string path, Reference<RavenJObject> metadata = null);
        Task<Stream> DownloadAsync(FileHeader path, Reference<RavenJObject> metadata = null);


        void RegisterUpload(string path, Stream stream, RavenJObject metadata = null, Etag etag = null);
        void RegisterUpload(FileHeader path, Stream stream, Etag etag = null);
        void RegisterUpload(string path, long fileSize, Action<Stream> write, RavenJObject metadata = null, Etag etag = null);
        void RegisterUpload(FileHeader path, long fileSize, Action<Stream> write, Etag etag = null);

        void RegisterFileDeletion(string path, Etag etag = null);
        void RegisterFileDeletion(FileHeader path, Etag etag = null);

		void RegisterDeletionQuery(string query, string[] orderByFields = null, int start = 0, int pageSize = int.MaxValue);

        void RegisterRename(string sourceFile, string destinationFile, Etag etag = null);
        void RegisterRename(FileHeader sourceFile, string destinationFile, Etag etag = null);

        /// <summary>
        /// Saves all the changes to the Raven server.
        /// </summary>
        Task SaveChangesAsync();
    }
}
