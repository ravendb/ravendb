using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16511 : RavenLowLevelTestBase
    {
        [Fact]
        public async Task ReplaceIndexShouldWork()
        {
            using (var database = CreateDocumentDatabase(runInMemory: false))
            {
                var testingStuff = database.IndexStore.ForTestingPurposesOnly();

                using (var index = MapIndex.CreateNew(new IndexDefinition() { Name = "Users_ByName", Maps = { "from user in docs.Users select new { user.Name }" }, },
                    database))
                {

                    index.IndexPersistence.LuceneDirectory.TempFileCache.SetMemoryStreamCapacity(1);
                    testingStuff.RunFakeIndex(index);

                    using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
                    {
                        using (var tx = context.OpenWriteTransaction())
                        {
                            using (var doc = CreateDocument(context, "users/1",
                                new DynamicJsonValue
                                {
                                    ["Name"] = "John",
                                    [Constants.Documents.Metadata.Key] = new DynamicJsonValue { [Constants.Documents.Metadata.Collection] = "Users" }
                                }))
                            {
                                database.DocumentsStorage.Put(context, "users/1", null, doc);
                            }

                            using (var doc = CreateDocument(context, "users/2",
                                new DynamicJsonValue
                                {
                                    ["Name"] = "Edward",
                                    [Constants.Documents.Metadata.Key] = new DynamicJsonValue { [Constants.Documents.Metadata.Collection] = "Users" }
                                }))
                            {
                                database.DocumentsStorage.Put(context, "users/2", null, doc);
                            }

                            tx.Commit();
                        }
                    }

                    while (index.HadRealIndexingWork == false)
                    {
                        await Task.Delay(100);
                    }

                    var mre = new SemaphoreSlim(initialCount: 0);

                    database.Changes.OnIndexChange += change =>
                    {
                        if (change.Type == IndexChangeTypes.SideBySideReplace)
                            mre.Release();
                    };

                    using (var newIndex = MapIndex.CreateNew(
                        new IndexDefinition()
                        {
                            Name = $"{Constants.Documents.Indexing.SideBySideIndexNamePrefix}Users_ByName",
                            Maps = { " from user in docs.Users select new { user.Name }" },
                        },
                        database))
                    {
                        newIndex.IndexPersistence.LuceneDirectory.TempFileCache.SetMemoryStreamCapacity(1);
                        testingStuff.RunFakeIndex(newIndex);

                        Assert.True(await mre.WaitAsync(TimeSpan.FromSeconds(15)), "Index wasn't replaced");

                        Assert.True(SpinWait.SpinUntil(() => newIndex.Status == IndexRunningStatus.Running, TimeSpan.FromSeconds(10)),
                            "newIndex.Status == IndexRunningStatus.Running");
                    }
                }

            }
        }

        public RavenDB_16511(ITestOutputHelper output) : base(output)
        {
        }
    }
}
