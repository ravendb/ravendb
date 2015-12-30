using System;
using System.Threading.Tasks;

using Raven.Abstractions.Database.Smuggler.FileSystem;

namespace Raven.Smuggler
{
    public class SmugglerFilesOperationDispatcher : SmugglerOperationDispatcher<FileSystemSmugglerOptions>
    {
        public SmugglerFilesOperationDispatcher()
            : base(null)
        {
            // TODO arek
        }

        protected override Task PerformImportAsync(FileSystemSmugglerOptions parameters)
        {
            throw new NotImplementedException();
        }

        protected override Task PerformExportAsync(FileSystemSmugglerOptions parameters)
        {
            throw new NotImplementedException();
        }

        protected override Task PerformBetweenAsync(FileSystemSmugglerOptions parameters)
        {
            throw new NotImplementedException();
        }
    }
}
