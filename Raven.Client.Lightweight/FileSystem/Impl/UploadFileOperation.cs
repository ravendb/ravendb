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
        public Action<Stream> StreamWriter { get; private set; }
        public RavenJObject Metadata { get; private set; }
        public Etag Etag { get; private set; }

        public UploadFileOperation(string path, Action<Stream> stream, RavenJObject metadata = null, Etag etag = null)
        {
            this.Path = path;
            this.StreamWriter = stream;
            this.Metadata = metadata;
            this.Etag = etag;
        }

        public async Task<bool> Execute(IAsyncFilesSession session)
        {
            var commands = session.Commands;

            //var pipe = new BlockingStream(20);            
            //var task = Task.Run(() => StreamWriter(pipe));

            //var upload = commands.UploadAsync(Path, Metadata, pipe)
            //                     .ContinueWith(x => (x.IsFaulted || x.IsCanceled) ? false : true)
            //                     .ConfigureAwait(false);

            //return await upload;

            throw new NotImplementedException();

        }
    }
}
