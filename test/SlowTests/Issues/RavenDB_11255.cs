using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11255 : RavenLowLevelTestBase
    {
        public RavenDB_11255(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_update_lock_mode_and_priority_of_index_even_if_indexing_is_running()
        {
            using (var database = CreateDocumentDatabase())
            {
                using (var index = MapIndex.CreateNew(new IndexDefinition()
                {
                    Name = "Users_ByName",
                    Maps =
                    {
                        "from user in docs.Users select new { user.Name }"
                    },
                    Type = IndexType.Map
                }, database))
                {
                    using (index._indexStorage._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (context.OpenWriteTransaction()) // open write tx to simulate running indexing batch
                    {
                        var task = Task.Factory.StartNew(() =>
                        {
                            index.SetLock(IndexLockMode.LockedIgnore);
                            index.SetPriority(IndexPriority.High);
                        }, TaskCreationOptions.LongRunning);

                        Assert.True(task.Wait(TimeSpan.FromMinutes(1)));
                    }

                    index.Start(); // will persist the introduced changes

                    IndexDefinition persistedDef = null;

                    for (int i = 0; i < 10; i++)
                    {
                        persistedDef = MapIndexDefinition.Load(index._indexStorage.Environment(), out var version);

                        if (persistedDef.Priority == IndexPriority.High)
                            break;

                        Thread.Sleep(1000);
                    }

                    Assert.Equal(IndexLockMode.LockedIgnore, persistedDef.LockMode);
                    Assert.Equal(IndexPriority.High, persistedDef.Priority);
                }
            }
        }
    }
}
