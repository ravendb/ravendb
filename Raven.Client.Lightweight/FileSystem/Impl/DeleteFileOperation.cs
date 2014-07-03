using Raven.Abstractions.Data;
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

        public string Path { get; private set; }

        public Etag Etag { get; private set; }

        public DeleteFileOperation(InMemoryFilesSessionOperations sessionOperations, string path, Etag etag)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentNullException("path", "The path cannot be null, empty or whitespace.");

            this.sessionOperations = sessionOperations;
            this.Path = path;
            this.Etag = etag;
        }

        public async Task Execute(IAsyncFilesSession session)
        {
            var commands = session.Commands;
            
            await commands.DeleteAsync(Path, Etag)
                          .ConfigureAwait(false);

            sessionOperations.RegisterMissing(Path);
        }
    }
}
