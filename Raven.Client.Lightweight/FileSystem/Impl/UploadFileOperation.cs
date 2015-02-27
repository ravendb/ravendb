using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Client.Util;
using Raven.Json.Linq;
using System;
using System.IO;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem.Impl
{
    internal class UploadFileOperation : IFilesOperation
    {
        protected readonly InMemoryFilesSessionOperations sessionOperations;

        public String Filename { get; set; }
        public RavenJObject Metadata { get; private set; }
        public Etag Etag { get; private set; }

        
        public long Size { get; private set; }
        public Action<Stream> StreamWriter { get; private set; }


        public UploadFileOperation(InMemoryFilesSessionOperations sessionOperations, string path, long size, Action<Stream> stream, RavenJObject metadata = null, Etag etag = null)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentNullException("path", "The path cannot be null, empty or whitespace.");

            this.sessionOperations = sessionOperations;

            this.Filename = path;
            this.Metadata = metadata;
            this.Etag = etag;

            this.StreamWriter = stream;
            this.Size = size;
        }

        public async Task<FileHeader> Execute(IAsyncFilesSession session)
        {
            var commands = session.Commands;

            var pipe = new BlockingStream(10);           

            Task.Run(() => StreamWriter(pipe))
                                .ContinueWith(x => pipe.CompleteWriting())
                                .ConfigureAwait(false);

            if (sessionOperations.EntityChanged(Filename))
            {
                if (!sessionOperations.IsDeleted(Filename))
                {
                    var fileHeaderInCache = await session.LoadFileAsync(Filename).ConfigureAwait(false);
                    Metadata = fileHeaderInCache.Metadata;
                }
            }

            await commands.UploadAsync(Filename, pipe, Metadata, Size)
                          .ConfigureAwait(false);

            var metadata = await commands.GetMetadataForAsync(Filename);
            if (metadata == null)
                return null;

            return new FileHeader(Filename, metadata);
        }
    }
}