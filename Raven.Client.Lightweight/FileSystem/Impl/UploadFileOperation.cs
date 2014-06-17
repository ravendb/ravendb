using Raven.Abstractions.Data;
using Raven.Client.Util;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem.Impl
{

    internal class UploadFileOperation : IFilesOperation
    {
        public string Path { get; private set; }
        public RavenJObject Metadata { get; private set; }
        public Etag Etag { get; private set; }

        
        public long Size { get; private set; }
        public Action<Stream> StreamWriter { get; private set; }


        public UploadFileOperation(string path, long size, Action<Stream> stream, RavenJObject metadata = null, Etag etag = null)
        {
            this.Path = path;
            this.Metadata = metadata;
            this.Etag = etag;

            this.StreamWriter = stream;
            this.Size = size;
        }

        public async Task<bool> Execute(IAsyncFilesSession session)
        {
            var commands = session.Commands;

            var pipe = new BlockingStream(10);
            var upload = commands.UploadAsync(Path, Metadata, pipe, Size)
                                 .ContinueWith(x => (x.IsFaulted || x.IsCanceled) ? false : true)
                                 .ConfigureAwait(false);
            
            var task = Task.Run(() => StreamWriter(pipe))
                           .ContinueWith(x => { pipe.CompleteWriting(); })
                           .ConfigureAwait(false);

            return await upload;
        }
    }
}
