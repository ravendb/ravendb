using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem.Impl
{
    internal class RenameFileOperation : IFilesOperation
    {
        public string Source { get; private set; }
        public string Destination { get; private set; }

        public RenameFileOperation(string sourcePath, string destinationPath)
        {
            this.Source = sourcePath;
            this.Destination = destinationPath;
        }
        public Task<bool> Execute(IAsyncFilesSession session)
        {
            throw new NotImplementedException();
        }
    }
}
