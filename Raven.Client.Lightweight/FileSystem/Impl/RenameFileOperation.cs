using Raven.Abstractions.FileSystem;
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

        public string Filename { get; set; }
        public string Destination { get; private set; }

        public RenameFileOperation(InMemoryFilesSessionOperations sessionOperations, string sourcePath, string destinationPath)
        {
            if (string.IsNullOrWhiteSpace(sourcePath))
                throw new ArgumentNullException("sourcePath", "The source path cannot be null, empty or whitespace.");

            if (string.IsNullOrWhiteSpace(destinationPath))
                throw new ArgumentNullException("destinationPath", "The destination path cannot be null, empty or whitespace.");

            this.sessionOperations = sessionOperations;
            this.Filename = sourcePath;
            this.Destination = destinationPath;
        }
        public async Task<FileHeader> Execute(IAsyncFilesSession session)
        {
            var commands = session.Commands;

            await commands.RenameAsync(Filename, Destination)
                          .ConfigureAwait(false);

            var metadata = await commands.GetMetadataForAsync(Filename);
            if (metadata == null)
                return null;

            return new FileHeader(Filename, metadata);
        }
    }
}
