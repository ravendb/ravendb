using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using System;
using System.Threading.Tasks;

namespace Raven.NewClient.Client.FileSystem.Impl
{
    internal class DeleteFileOperation : IFilesOperation
    {
        private readonly InMemoryFilesSessionOperations sessionOperations;

        public string FileName { get; private set; }

        private long? Etag { get; set; }

        public DeleteFileOperation(InMemoryFilesSessionOperations sessionOperations, string path, long? etag)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentNullException("path", "The path cannot be null, empty or whitespace.");

            this.sessionOperations = sessionOperations;
            this.FileName = path;
            this.Etag = etag;
        }

        public async Task<FileHeader> Execute(IAsyncFilesSession session)
        {
            var commands = session.Commands;

            bool delete = true;
            
            FileHeader fileHeader = null;
            if (!sessionOperations.TryGetFromCache(FileName, out fileHeader))
                fileHeader = await session.LoadFileAsync(FileName).ConfigureAwait(false);

            foreach (var deleteListener in sessionOperations.Listeners.DeleteListeners)
            {
                if (!deleteListener.BeforeDelete(fileHeader))
                    delete = false;
            }

            if (delete)
            {
                await commands.DeleteAsync(FileName, Etag)
                              .ConfigureAwait(false);

                sessionOperations.RegisterMissing(FileName);

                foreach (var deleteListener in sessionOperations.Listeners.DeleteListeners)
                {
                    deleteListener.AfterDelete(FileName);
                }
            }

            return null;
        }
    }
}
