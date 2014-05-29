using Raven.Abstractions.Data;
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


        Task<T> LoadAsync<T>(string path) where T : IRemoteObject;
        Task<T[]> LoadAsync<T>(IEnumerable<string> paths) where T : IRemoteObject;
        Task<T[]> LoadAsync<T>(params string[] paths) where T : IRemoteObject;

        /// <summary>
        /// This version of the create will use the conventions for creation specified for each T type.
        /// </summary>
        /// <typeparam name="T">Either a file or a directory</typeparam>
        /// <param name="directory">The container</param>
        /// <param name="name">The name of the object</param>
        /// <returns>The object itself</returns>
        Task<T> CreateAsync<T>(RemoteDirectory directory, string name) where T : IRemoteObject;

        Task<RemoteFile> CreateAsync(RemoteDirectory directory, string name, CreateFileOptions options, Stream data = null, Etag etag = null);
        Task<RemoteFile> CreateAsync(string name, CreateFileOptions options, Stream data = null, Etag etag = null);

        /// <summary>
        /// Marks the specified file for deletion. The file will be deleted when <see cref="IFilesSession.SaveChanges"/> is called.
        /// </summary>
        /// <param name="file">The file.</param>
        void DeleteAsync(RemoteFile file, DeleteFileOptions options);

        /// <summary>
        /// Marks the specified directory for deletion. The directory will be deleted when <see cref="IFilesSession.SaveChanges"/> is called.
        /// </summary>
        /// <param name="file">The file.</param>
        void DeleteAsync(RemoteDirectory file, DeleteDirectoryOptions options);


        Task WriteAsync(RemoteFile file, Action<Stream> writeAction);

        Task<Stream> ReadAsync(RemoteFile file);

        Task<T> RenameAsync<T>(T file, string name) where T : IRemoteObject;
        Task<T> MoveAsync<T>(T file, RemoteDirectory destination) where T : IRemoteObject;

        /// <summary>
        /// Saves all the changes to the Raven server.
        /// </summary>
        Task SaveChangesAsync();
    }
}
