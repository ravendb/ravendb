using Raven.Abstractions.Data;
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
        public Action<Stream> Stream { get; private set; }
        public RavenJObject Metadata { get; private set; }
        public Etag Etag { get; private set; }

        public UploadFileOperation(string path, Action<Stream> stream, RavenJObject metadata = null, Etag etag = null)
        {
            this.Path = path;
            this.Stream = stream;
            this.Metadata = metadata;
            this.Etag = etag;
        }

        public Task<bool> Execute(IAsyncFilesSession session)
        {
            throw new NotImplementedException();
        }
    }
}
