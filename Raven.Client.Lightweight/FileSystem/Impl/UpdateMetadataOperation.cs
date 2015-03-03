using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem.Impl
{
    public class UpdateMetadataOperation: IFilesOperation
    {
        protected readonly InMemoryFilesSessionOperations sessionOperations;
        public String Filename { get; set; }
        public FileHeader FileHeader { get; private set; }
        public RavenJObject Metadata { get; private set; }
		public Etag Etag { get; private set; }

		public UpdateMetadataOperation(InMemoryFilesSessionOperations sessionOperations, FileHeader fileHeader, RavenJObject metadata, Etag etag)
        {
            if (fileHeader != null && string.IsNullOrWhiteSpace(fileHeader.FullPath))
                throw new ArgumentNullException("fileHeader", "The file cannot be null or have an empty or whitespace name.");

            this.sessionOperations = sessionOperations;

            FileHeader = fileHeader;
            Filename = fileHeader.FullPath;
            Metadata = metadata;
			Etag = etag;
        }

        public async Task<FileHeader> Execute(IAsyncFilesSession session)
        {
            var commands = session.Commands;

            if (sessionOperations.IsDeleted(Filename))
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
                await commands.UpdateMetadataAsync(Filename, Metadata, Etag);
                
                foreach ( var listener in sessionOperations.Listeners.MetadataChangeListeners )
                {
                    listener.AfterChange(FileHeader, Metadata);
                }
            }

            var metadata = await commands.GetMetadataForAsync(Filename);
            if (metadata == null)
                return null;

            return new FileHeader(Filename, metadata);
        }
    }
}
