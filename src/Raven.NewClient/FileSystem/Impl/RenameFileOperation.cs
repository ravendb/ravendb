using Raven.NewClient.Abstractions.FileSystem;
using System;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Data;

namespace Raven.NewClient.Client.FileSystem.Impl
{
    internal class RenameFileOperation : IFilesOperation
    {
        private readonly InMemoryFilesSessionOperations sessionOperations;

        public string FileName { get; private set; }
        private string Destination { get; set; }
        private long? Etag { get; set; }

        public RenameFileOperation(InMemoryFilesSessionOperations sessionOperations, string sourcePath, string destinationPath, long? etag)
        {
            if (string.IsNullOrWhiteSpace(sourcePath))
                throw new ArgumentNullException("sourcePath", "The source path cannot be null, empty or whitespace!");

            if (string.IsNullOrWhiteSpace(destinationPath))
                throw new ArgumentNullException("destinationPath", "The destination path cannot be null, empty or whitespace!");

            this.sessionOperations = sessionOperations;
            this.FileName = sourcePath;
            this.Destination = destinationPath;
            this.Etag = etag;
        }
        public async Task<FileHeader> Execute(IAsyncFilesSession session)
        {
            var commands = session.Commands;

            await commands.RenameAsync(FileName, Destination, Etag)
                          .ConfigureAwait(false);

            var metadata = await commands.GetMetadataForAsync(Destination).ConfigureAwait(false);
            if (metadata == null)
                return null;

            return new FileHeader(Destination, metadata);
        }
    }
}
