using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem.Impl
{
    internal class DeleteFileOperation : IFilesOperation
    {
        protected readonly InMemoryFilesSessionOperations sessionOperations;

        public string Filename { get; set; }

        public Etag Etag { get; private set; }

        public DeleteFileOperation(InMemoryFilesSessionOperations sessionOperations, string path, Etag etag)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentNullException("path", "The path cannot be null, empty or whitespace.");

            this.sessionOperations = sessionOperations;
            this.Filename = path;
            this.Etag = etag;
        }

        public async Task<FileHeader> Execute(IAsyncFilesSession session)
        {
            var commands = session.Commands;

            bool delete = true;
            
            FileHeader fileHeader = null;
            if (!sessionOperations.TryGetFromCache(Filename, out fileHeader))
                fileHeader = await session.LoadFileAsync(Filename);

            foreach (var deleteListener in sessionOperations.Listeners.DeleteListeners)
            {
                if (!deleteListener.BeforeDelete(fileHeader))
                    delete = false;
            }

            if (delete)
            {
                await commands.DeleteAsync(Filename)
                              .ConfigureAwait(false);

                sessionOperations.RegisterMissing(Filename);

                foreach (var deleteListener in sessionOperations.Listeners.DeleteListeners)
                {
                    deleteListener.AfterDelete(Filename);
                }
            }

            return null;
        }
    }
}
