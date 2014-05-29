using Raven.Abstractions.Data;
using Raven.Client.Connection.Profiling;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem
{
    ///<summary>
    /// Expose the set of operations by the RavenDB server
    ///</summary>
    public interface IFilesSession 
    {
        /// <summary>
        /// Get the accessor for advanced operations
        /// </summary>
        /// <remarks>
        /// Those operations are rarely needed, and have been moved to a separate 
        /// property to avoid cluttering the API
        /// </remarks>
        IAdvancedFilesSessionOperations Advanced { get; }


        T Load<T>(string path) where T : IRemoteObject;
        T[] Load<T>(IEnumerable<string> paths) where T : IRemoteObject;
        T[] Load<T>(params string[] paths) where T : IRemoteObject;

        RemoteFile Create(RemoteDirectory directory, string name, CreateFileOptions options, Stream data = null, Etag etag = null);

        RemoteFile Create(string name, CreateFileOptions options, Stream data = null, Etag etag = null);

        /// <summary>
        /// Marks the specified file for deletion. The file will be deleted when <see cref="IFilesSession.SaveChanges"/> is called.
        /// </summary>
        /// <param name="file">The file.</param>
        void Delete(RemoteFile file, DeleteFileOptions options);

        /// <summary>
        /// Marks the specified directory for deletion. The directory will be deleted when <see cref="IFilesSession.SaveChanges"/> is called.
        /// </summary>
        /// <param name="file">The file.</param>
        void Delete(RemoteDirectory file, DeleteDirectoryOptions options);


        void Write(RemoteFile file, Action<Stream> writeAction);

        Stream Read(RemoteFile file);


        RemoteFile Rename(RemoteFile file, string name);
        RemoteFile Move(RemoteFile file, RemoteDirectory destination);

        /// <summary>
        /// Saves all the changes to the Raven server.
        /// </summary>
        void SaveChanges();
    }
}
