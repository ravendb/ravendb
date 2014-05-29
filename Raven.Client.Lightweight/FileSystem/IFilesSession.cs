using Raven.Abstractions.Data;
using Raven.Client.Connection.Profiling;
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

        FileHeader LoadFile(string path);
        FileHeader LoadFile(DirectoryHeader directory, string filename);

        FileHeader LoadFiles(IEnumerable<string> path);

        // TODO: Paging or streaming?
        // FileHeader LoadFiles(DirectoryHeader directory);

        DirectoryHeader LoadDirectory(string path);

        Stream Download(string path);
        Stream Download(FileHeader path);


        void RegisterUpload(string path, Stream stream, RavenJObject metadata = null, Etag etag = null);
        void RegisterUpload(FileHeader path, Stream stream, RavenJObject metadata = null, Etag etag = null);
        void RegisterUpload(string path, long start, Action<Stream> write, RavenJObject metadata = null, Etag etag = null);
        void RegisterUpload(FileHeader path, long start, Action<Stream> write, RavenJObject metadata = null, Etag etag = null);

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
        void SaveChanges();
    }
}
