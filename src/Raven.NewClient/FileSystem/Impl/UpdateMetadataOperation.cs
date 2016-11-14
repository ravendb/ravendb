using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Abstractions.FileSystem;
using Raven.NewClient.Json.Linq;
using System;
using System.Threading.Tasks;

namespace Raven.NewClient.Client.FileSystem.Impl
{
    public class UpdateMetadataOperation: IFilesOperation
    {
        private readonly InMemoryFilesSessionOperations sessionOperations;

        public string FileName { get; private set; }
        private FileHeader FileHeader { get; set; }
        private RavenJObject Metadata { get; set; }
        private long? Etag { get; set; }

        public UpdateMetadataOperation(InMemoryFilesSessionOperations sessionOperations, FileHeader fileHeader, RavenJObject metadata, long? etag)
        {
            if (fileHeader == null || string.IsNullOrWhiteSpace(fileHeader.FullPath))
                throw new ArgumentNullException("fileHeader", "The file cannot be null or have an empty or whitespace name!");

            this.sessionOperations = sessionOperations;

            FileHeader = fileHeader;
            FileName = fileHeader.FullPath;
            Metadata = metadata;
            Etag = etag;
        }

        public async Task<FileHeader> Execute(IAsyncFilesSession session)
        {
            var commands = session.Commands;

            if (sessionOperations.IsDeleted(FileName))
                return null;

            bool update = true;
            foreach ( var listener in sessionOperations.Listeners.MetadataChangeListeners )
            {
                if (!listener.BeforeChange( FileHeader, Metadata, FileHeader.OriginalMetadata))
                {
                    update = false;
                }
            }

            if (update)
            {
                await commands.UpdateMetadataAsync(FileName, Metadata, Etag).ConfigureAwait(false);
                
                foreach ( var listener in sessionOperations.Listeners.MetadataChangeListeners )
                {
                    listener.AfterChange(FileHeader, Metadata);
                }
            }

            var metadata = await commands.GetMetadataForAsync(FileName).ConfigureAwait(false);
            if (metadata == null)
                return null;

            return new FileHeader(FileName, metadata);
        }
    }
}
