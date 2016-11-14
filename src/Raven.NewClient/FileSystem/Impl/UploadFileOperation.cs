using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Abstractions.FileSystem;
using Raven.NewClient.Client.Util;
using Raven.NewClient.Json.Linq;
using System;
using System.IO;
using System.Threading.Tasks;

namespace Raven.NewClient.Client.FileSystem.Impl
{
    internal class UploadFileOperation : IFilesOperation
    {
        private readonly InMemoryFilesSessionOperations sessionOperations;

        public string FileName { get; private set; }
        private RavenJObject Metadata { get; set; }
        public long? Etag { get; internal set; }


        private long Size { get; set; }
        private Action<Stream> StreamWriter { get; set; }

        private Stream Stream { get; set; }

        private UploadFileOperation(InMemoryFilesSessionOperations sessionOperations, string path, RavenJObject metadata = null, long? etag = null)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentNullException("path", "The path cannot be null, empty or whitespace!");

            this.sessionOperations = sessionOperations;

            FileName = path;
            Metadata = metadata;
            Etag = etag;
        }

        public UploadFileOperation(InMemoryFilesSessionOperations sessionOperations, string path, Stream stream, RavenJObject metadata = null, long? etag = null)
            : this(sessionOperations, path, metadata, etag)
        {
            Stream = stream;
        }

        public UploadFileOperation(InMemoryFilesSessionOperations sessionOperations, string path, long size, Action<Stream> stream, RavenJObject metadata = null, long? etag = null)
            : this(sessionOperations, path, metadata, etag)
        {
            StreamWriter = stream;
            Size = size;
        }

        public async Task<FileHeader> Execute(IAsyncFilesSession session)
        {
            var commands = session.Commands;

            if (sessionOperations.EntityChanged(FileName))
            {
                if (!sessionOperations.IsDeleted(FileName))
                {
                    var fileHeaderInCache = await session.LoadFileAsync(FileName).ConfigureAwait(false);
                    Metadata = fileHeaderInCache.Metadata;
                }
            }

            if (Stream != null)
            {
                await commands.UploadAsync(FileName, Stream, Metadata, Etag)
                          .ConfigureAwait(false);
            }
            else if (StreamWriter != null)
            {
                await commands.UploadAsync(FileName, StreamWriter, null, Size, Metadata, Etag)
                    .ConfigureAwait(false);
            }
            else
            {
                throw new InvalidOperationException("Neither stream not stream writer was specified");
            }

            var metadata = await commands.GetMetadataForAsync(FileName).ConfigureAwait(false);
            if (metadata == null)
                return null;

            return new FileHeader(FileName, metadata);
        }
    }
}
