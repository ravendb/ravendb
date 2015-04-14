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
	    private readonly InMemoryFilesSessionOperations sessionOperations;

        public string FileName { get; private set; }
	    private RavenJObject Metadata { get; set; }
	    private Etag Etag { get; set; }


	    private long Size { get; set; }
	    private Action<Stream> StreamWriter { get; set; }


        public UploadFileOperation(InMemoryFilesSessionOperations sessionOperations, string path, long size, Action<Stream> stream, RavenJObject metadata = null, Etag etag = null)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentNullException("path", "The path cannot be null, empty or whitespace!");

            this.sessionOperations = sessionOperations;

            this.FileName = path;
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

            if (sessionOperations.EntityChanged(FileName))
            {
                if (!sessionOperations.IsDeleted(FileName))
                {
                    var fileHeaderInCache = await session.LoadFileAsync(FileName).ConfigureAwait(false);
                    Metadata = fileHeaderInCache.Metadata;
                }
            }

            await commands.UploadAsync(FileName, pipe, Metadata, Size, Etag)
                          .ConfigureAwait(false);

            var metadata = await commands.GetMetadataForAsync(FileName).ConfigureAwait(false);
            if (metadata == null)
                return null;

            return new FileHeader(FileName, metadata);
        }
    }
}