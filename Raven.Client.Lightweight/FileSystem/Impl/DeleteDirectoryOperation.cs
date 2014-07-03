using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem.Impl
{
    internal class DeleteDirectoryOperation : IFilesOperation
    {
        protected readonly InMemoryFilesSessionOperations sessionOperations;

        public string Path { get; private set; }

        public bool Recurse { get; private set; }

        public DeleteDirectoryOperation(InMemoryFilesSessionOperations sessionOperations, string path, bool recurse)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentNullException("path", "The path cannot be null, empty or whitespace.");

            this.sessionOperations = sessionOperations;
            this.Path = path;
            this.Recurse = recurse;
        }

        public Task Execute(IAsyncFilesSession session)
        {
            throw new NotImplementedException();
        }
    }
}
