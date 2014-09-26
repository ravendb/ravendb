using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Smuggler;
using Raven.Smuggler;
using Raven.Tests.Helpers;
using Xunit;

namespace RavenFS.Tests.Smuggler
{
    public class SmugglerBetweenOperationTests : RavenFilesTestWithLogs
    {
        [Fact, Trait("Category", "Smuggler")]
        public async Task BetweenOperationShouldNotCreateFilesystem()
        {
            using (var store = NewStore())
            {
                var smugglerApi = new SmugglerFilesApi();                

                var options = new SmugglerBetweenOptions<FilesConnectionStringOptions>
                {
                    From = new FilesConnectionStringOptions
                    {
                        Url = store.Url,
                        DefaultFileSystem = "DB1"
                    },
                    To = new FilesConnectionStringOptions
                    {
                        Url = store.Url,
                        DefaultFileSystem = "DB2"
                    }
                };

                var aggregateException = TaskAssert.Throws<AggregateException>(() => smugglerApi.Between(options));
                var exception = aggregateException.ExtractSingleInnerException();
                Assert.True(exception.Message.StartsWith("Smuggler does not support filesystem creation (filesystem 'DB1' on server"));

                await store.AsyncFilesCommands.Admin.EnsureFileSystemExistsAsync("DB1");

                aggregateException = TaskAssert.Throws<AggregateException>(() => smugglerApi.Between(options));
                exception = aggregateException.ExtractSingleInnerException();
                Assert.True(exception.Message.StartsWith("Smuggler does not support filesystem creation (filesystem 'DB2' on server"));
            }
        }
    }
}
