using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace RavenFS.Tests.Smuggler
{
    public class SmugglerExecutionTests
    {
        [Fact, Trait("Category", "Smuggler")]
        public async Task ShouldThrowIfFileSystemDoesNotExist()
        {
            throw new NotImplementedException();
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task ShouldNotThrowIfFileSystemExists()
        {
            throw new NotImplementedException();
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task ShouldNotThrowIfFileSystemExistsUsingDefaultConfiguration()
        {
            throw new NotImplementedException();
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task BehaviorWhenServerIsDown()
        {
            throw new NotImplementedException();
        }

        [Fact, Trait("Category", "Smuggler")]
        public void MaxChunkSizeInMbShouldBeRespectedByDataDumper()
        {
            throw new NotImplementedException();
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanDumpEmptyDatabase()
        {
            throw new NotImplementedException();
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanHandleFilesExceptionsGracefully()
        {
            throw new NotImplementedException();
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanImportFromDumpFile()
        {
            throw new NotImplementedException();
        }
    }
}
