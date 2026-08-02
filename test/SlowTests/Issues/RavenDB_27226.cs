using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_27226 : RavenTestBase
    {
        public RavenDB_27226(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public async Task DisposingIndex_WaitsForTheRunningQueriesToEnd_AndThenReleasesItsStorageEnvironment()
        {
            // the index must live on disk - the whole point is that its memory mapped files keep the
            // index directory undeletable on Windows if the storage environment is not disposed
            using (var store = GetDocumentStore(new Options { RunInMemory = false, Path = NewDataPath() }))
            {
                const string indexName = "Users/ByName";

                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Maps = { "from user in docs.Users select new { user.Name }" },
                    Type = IndexType.Map,
                    Name = indexName
                }));

                Indexes.WaitForIndexing(store);

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                var index = database.IndexStore.GetIndex(indexName);

                var indexPath = index._environment.Options.BasePath.FullPath;
                Assert.True(Directory.Exists(indexPath), $"Expected the index directory '{indexPath}' to exist");

                Task delete;

                using (index.ForTestingPurposesOnly().HoldRunningQueriesReadLock())
                {
                    delete = Task.Run(() => database.IndexStore.DeleteIndexInternal(index));

                    // longer than the 10 seconds DrainRunningQueries is willing to wait, so giving up on the
                    // drain and disposing the index anyway would be caught here. a query is still reading from
                    // the index, disposing it now would pull its readers and environment from underneath it
                    Assert.False(delete.Wait(TimeSpan.FromSeconds(15)),
                        $"Index '{index.Name}' was disposed while a query was still running on it");

                    Assert.False(index._environment.Disposed,
                        $"The storage environment of '{index.Name}' was disposed while a query was still running on it");
                }

                // the query is done, nothing is reading from the index anymore, so the dispose can complete
                await delete.WaitAsync(TimeSpan.FromSeconds(30));

                // without this the environment stays alive with its temp buffers mapped, the index is no longer
                // reachable through IndexStore so nothing can ever dispose it, and on Windows neither the file nor
                // the index directory can be deleted for the lifetime of the process
                Assert.True(index._environment.Disposed,
                    $"The storage environment of '{index.Name}' was not disposed, so its memory mapped files are leaked for the lifetime of the process");

                Assert.False(Directory.Exists(indexPath), $"Expected the index directory '{indexPath}' to be deleted");
            }
        }
    }
}
