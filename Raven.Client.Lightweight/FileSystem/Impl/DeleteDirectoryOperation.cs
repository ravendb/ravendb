using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem.Impl
{
    internal class DeleteDirectoryOperation : IFilesOperation
    {
        public string Path { get; private set; }

        public bool Recurse { get; private set; }

        public DeleteDirectoryOperation(string path, bool recurse)
        {
            this.Path = path;
            this.Recurse = recurse;
        }

        public Task Execute(IAsyncFilesSession session)
        {
            throw new NotImplementedException();
        }
    }
}
