using Raven.Abstractions.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem.Impl
{
    internal class DeleteFileOperation : IFilesOperation
    {
        public string Path { get; private set; }

        public Etag Etag { get; private set; }

        public DeleteFileOperation(string path, Etag etag)
        {
            this.Path = path;
            this.Etag = etag;
        }

        public Task Execute(IAsyncFilesSession session)
        {
            throw new NotImplementedException();
        }
    }
}
