using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem.Impl
{
    internal class RenameFileOperation : IFilesOperation
    {
        protected readonly InMemoryFilesSessionOperations sessionOperations;

        public string Source { get; private set; }
        public string Destination { get; private set; }

        public RenameFileOperation(InMemoryFilesSessionOperations sessionOperations, string sourcePath, string destinationPath)
        {
            if (string.IsNullOrWhiteSpace(sourcePath))
                throw new ArgumentNullException("sourcePath", "The source path cannot be null, empty or whitespace.");

            if (string.IsNullOrWhiteSpace(destinationPath))
                throw new ArgumentNullException("destinationPath", "The destination path cannot be null, empty or whitespace.");

            this.sessionOperations = sessionOperations;
            this.Source = sourcePath;
            this.Destination = destinationPath;
        }
        public async Task Execute(IAsyncFilesSession session)
        {
            var commands = session.Commands;

            await commands.RenameAsync(Source, Destination)
                          .ConfigureAwait(false);

            sessionOperations.RegisterMissing(Source);            
        }
    }
}
