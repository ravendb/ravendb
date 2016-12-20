// -----------------------------------------------------------------------
//  <copyright file="RavenDB-4222.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Database.Tasks;
using Raven.Tests.Common;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
    public class RavenDB_4222 : RavenTest
    {
        [Theory]
        [PropertyData("Storages")]
        public void SkipTasksForDisabledIndexes1(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => accessor.Tasks.AddTask(new RemoveFromIndexTask(101), DateTime.Now));

                storage.Batch(accessor =>
                {
                    var idsToSkip = new List<int>()
                    {
                        101
                    };

                    var task = accessor.Tasks.GetMergedTask<RemoveFromIndexTask>(idsToSkip, new[] {101}, new HashSet<IComparable>());
                    Assert.Null(task);
                });

                storage.Batch(accessor =>
                {
                    Assert.True(accessor.Tasks.HasTasks);
                    Assert.Equal(1, accessor.Tasks.ApproximateTaskCount);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void SkipTasksForDisabledIndexes2(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => accessor.Tasks.AddTask(new RemoveFromIndexTask(101), DateTime.Now));
                storage.Batch(accessor => accessor.Tasks.AddTask(new RemoveFromIndexTask(102), DateTime.Now));
                storage.Batch(accessor => accessor.Tasks.AddTask(new RemoveFromIndexTask(103), DateTime.Now));

                storage.Batch(accessor =>
                {
                    var idsToSkip = new List<int>()
                    {
                        102
                    };
                    var allIndexes = new[] {101, 102, 103};
                    var alreadySeen = new HashSet<IComparable>();
                    var task = accessor.Tasks.GetMergedTask<RemoveFromIndexTask>(idsToSkip, allIndexes, alreadySeen);
                    Assert.NotNull(task);
                    Assert.Equal(101, task.Index);
                    accessor.Tasks.DeleteTasks(alreadySeen);

                    task = accessor.Tasks.GetMergedTask<RemoveFromIndexTask>(idsToSkip, allIndexes, alreadySeen);
                    Assert.NotNull(task);
                    Assert.Equal(103, task.Index);
                    accessor.Tasks.DeleteTasks(alreadySeen);

                    task = accessor.Tasks.GetMergedTask<RemoveFromIndexTask>(idsToSkip, allIndexes, alreadySeen);
                    Assert.Null(task);
                });

                storage.Batch(accessor =>
                {
                    Assert.True(accessor.Tasks.HasTasks);
                    Assert.Equal(1, accessor.Tasks.ApproximateTaskCount);

                    var tasks = accessor.Tasks.GetPendingTasksForDebug().ToList();
                    Assert.Equal(1, tasks.Count);
                    Assert.Equal(102, tasks.First().IndexId);
                });
            }
        }

        [Fact]
        public void DontUpdateDisabledIndex()
        {
            using (var store = NewDocumentStore())
            {
                var indexName = "test";
                store.DatabaseCommands.PutIndex(indexName, new IndexDefinition
                {
                    Name = indexName,
                    Map = @"from doc in docs.Orders
select new{
doc.Name
}"
                });

                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Name = indexName
                    }, "orders/1");
                    session.SaveChanges();
                }

                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var result = session.Query<Order>(indexName).ToList();
                    Assert.Equal(1, result.Count);
                }
                var stats = store.DatabaseCommands.GetStatistics();
                Assert.Equal(1, stats.CountOfDocuments);

                store.DatabaseCommands.SetIndexPriority(indexName, IndexingPriority.Disabled);

                using (var session = store.OpenSession())
                {
                    session.Delete("orders/1");
                    session.SaveChanges();
                }

                WaitForIndexing(store);
                stats = store.DatabaseCommands.GetStatistics();
                Assert.Equal(0, stats.CountOfDocuments);

                var testIndex = stats.Indexes.First(x => x.Name == indexName);
                Assert.Equal(1, testIndex.DocsCount);
            }
        }

        private class Order
        {
            public string Name { get; set; }
        }
    }
}