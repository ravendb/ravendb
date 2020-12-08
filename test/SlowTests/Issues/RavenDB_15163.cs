using System;
using System.IO;
using System.Linq;
using System.Threading;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15163 : RavenTestBase
    {
        public RavenDB_15163(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void FailureDuringIndexReplacementMustNotCauseProblemsWith()
        {
            using (var store = GetDocumentStore(new Options {Path = NewDataPath()}))
            {
                var database = GetDatabase(store.Database).Result;

                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Maps = { "from user in docs.Users select new { user.FirstName }" },
                    Type = IndexType.Map,
                    Name = "Users/ByName"
                }));

                store.Maintenance.Send(new StopIndexingOperation());

                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Maps = { "from user in docs.Users select new { user.LastName }" },
                    Type = IndexType.Map,
                    Name = "Users/ByName"
                }));

                var replacementIndexInstance = database.IndexStore.GetIndex("ReplacementOf/Users/ByName");

                var thrown = false;

                database.IndexStore.ForTestingPurposesOnly().DuringIndexReplacement_AfterUpdatingCollectionOfIndexes += () =>
                {
                    if (thrown == false)
                    {
                        thrown = true;
                        throw new InvalidOperationException("Intentional failure during replacement");
                    }

                    database.IndexStore.ForTestingPurposesOnly().DuringIndexReplacement_AfterUpdatingCollectionOfIndexes = null;
                };

                var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

                database.IndexStore.ReplaceIndexes("Users/ByName", "ReplacementOf/Users/ByName", cts.Token);

                var indexes = database.IndexStore.GetIndexes().ToList();

                Assert.Equal(1, indexes.Count);
                Assert.Equal("Users/ByName", indexes[0].Name);
                Assert.Same(replacementIndexInstance, indexes[0]);

                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Maps = { "from user in docs.Users select new { user.LastName2 }" },
                    Type = IndexType.Map,
                    Name = "Users/ByName"
                }));

                replacementIndexInstance = database.IndexStore.GetIndex("ReplacementOf/Users/ByName");

                database.IndexStore.ForTestingPurposesOnly().DuringIndexReplacement_OnOldIndexDeletion += () =>
                {
                    // intentionally throw IOException error, the idea is that we let the replacement index to continue to run
                    // after the db restart it will manage to move to correct directory

                    throw new IOException("Intentional failure during replacement"); 
                };

                cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

                Assert.Throws<IOException>(() => database.IndexStore.ReplaceIndexes("Users/ByName", "ReplacementOf/Users/ByName", cts.Token));

                indexes = database.IndexStore.GetIndexes().ToList();

                Assert.Equal(1, indexes.Count);
                Assert.Equal("Users/ByName", indexes[0].Name);
                Assert.Same(replacementIndexInstance, indexes[0]);

                // restart db

                database.Dispose();

                Server.ServerStore.DatabasesLandlord.UnloadDirectly(database.Name);

                database = GetDatabase(store.Database).Result;

                WaitForIndexing(store); // old index could be opened as well, so we wait until replacement is done and switches the index

                indexes = database.IndexStore.GetIndexes().ToList();

                Assert.Equal(1, indexes.Count);
                Assert.Equal("Users/ByName", indexes[0].Name);
                Assert.Contains("LastName2", indexes[0].Definition.MapFields.Keys);
            }
        }
    }
}
