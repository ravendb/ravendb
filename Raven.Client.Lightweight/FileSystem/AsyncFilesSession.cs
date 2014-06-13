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
    public class AsyncFilesSession : InMemoryFilesSessionOperations, IAsyncFilesSession, IAsyncAdvancedFilesSessionOperations
    {
        	/// <summary>
		/// Initializes a new instance of the <see cref="AsyncFilesSession"/> class.
		/// </summary>
        public AsyncFilesSession(FilesStore filesStore,
                                 IAsyncFilesCommands asyncFilesCommands,
								 FilesSessionListeners listeners,
								 Guid id)
			: base(filesStore, listeners, id)
		{
            AsyncFilesCommands = asyncFilesCommands;
		}

        /// <summary>
        /// Gets the async files commands.
        /// </summary>
        /// <value>The async files commands.</value>
        public IAsyncFilesCommands AsyncFilesCommands { get; private set; }

        public override string FileSystemName
        {
            get { return AsyncFilesCommands.FileSystem; }
        }

        public IAsyncAdvancedFilesSessionOperations Advanced
        {
            get { return this; }
        }

        public Task<FileHeader> LoadFileAsync(string path)
        {
            throw new NotImplementedException();
        }

        public Task<FileHeader> LoadFileAsync(DirectoryHeader directory, string filename)
        {
            throw new NotImplementedException();
        }

        public Task<FileHeader[]> LoadFilesAsync(IEnumerable<string> path)
        {
            throw new NotImplementedException();
        }

        public Task<DirectoryHeader> LoadDirectoryAsync(string path)
        {
            throw new NotImplementedException();
        }

        public Task<Stream> DownloadAsync(string path)
        {
            throw new NotImplementedException();
        }

        public Task<Stream> DownloadAsync(FileHeader path)
        {
            throw new NotImplementedException();
        }
    }
}
