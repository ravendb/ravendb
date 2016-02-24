// -----------------------------------------------------------------------
//  <copyright file="TasksStorageActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Tests.Common;

namespace Raven.Tests.Storage
{
    using System;
    using Database.Tasks;
    using Xunit;
    using Xunit.Extensions;

    [Trait("VoronTest", "StorageActionsTests")]
    public class TasksStorageActionsTests : TransactionalStorageTestBase
    {
        [Theory]
        [PropertyData("Storages")]
        public void SimpleTask(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => accessor.Tasks.AddTask(new RemoveFromIndexTask(101), DateTime.Now));

                storage.Batch(accessor =>
                {
                    var task = accessor.Tasks.GetMergedTask<RemoveFromIndexTask>(new List<int>(), new[] {101});
                    Assert.NotNull(task);
                });

                storage.Batch(accessor =>
                {
                    Assert.False(accessor.Tasks.HasTasks);
                    Assert.Equal(0, accessor.Tasks.ApproximateTaskCount);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void MergingTask(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => accessor.Tasks.AddTask(new RemoveFromIndexTask(101), DateTime.Now));
                storage.Batch(accessor => accessor.Tasks.AddTask(new RemoveFromIndexTask(101), DateTime.Now));

                storage.Batch(accessor =>
                {
                    var task = accessor.Tasks.GetMergedTask<RemoveFromIndexTask>(new List<int>(), new[] {101});
                    Assert.NotNull(task);
                });

                storage.Batch(accessor =>
                {
                    Assert.False(accessor.Tasks.HasTasks);
                    Assert.Equal(0, accessor.Tasks.ApproximateTaskCount);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void CanAddAndRemoveMultipleTasks_InSingleTx(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(actions =>
                {
                    for (int i = 0; i < 3; i++)
                    {
                        var task = new RemoveFromIndexTask(100);
                        task.AddKey("tasks/" + i);
                        actions.Tasks.AddTask(task, SystemTime.UtcNow);
                    }
                });

                storage.Batch(actions =>
                {
                    var task = actions.Tasks.GetMergedTask<RemoveFromIndexTask>(new List<int>(), new[] {100});
                    Assert.NotNull(task);

                    task = actions.Tasks.GetMergedTask<RemoveFromIndexTask>(new List<int>(), new[] {100});
                    Assert.Null(task);
                });

                storage.Batch(accessor =>
                {
                    Assert.False(accessor.Tasks.HasTasks);
                    Assert.Equal(0, accessor.Tasks.ApproximateTaskCount);
                });

                storage.Batch(actions =>
                {
                    var isIndexStale = actions.Staleness.IsIndexStale(100, null, null);
                    Assert.False(isIndexStale);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void CanAddAndRemoveMultipleTasks_DifferentTypes(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(actions =>
                {
                    for (int i = 0; i < 3; i++)
                    {
                        var task = new RemoveFromIndexTask(100);
                        task.AddKey("tasks/" + i);
                        actions.Tasks.AddTask(task, SystemTime.UtcNow);
                    }

                    for (int i = 0; i < 3; i++)
                    {
                        var task = new TouchReferenceDocumentIfChangedTask(100);
                        actions.Tasks.AddTask(task, SystemTime.UtcNow);
                    }
                });

                storage.Batch(actions =>
                {
                    var idsToSkip = new List<int>();
                    var allIndexes = new[] { 100 };

                    var task1 = actions.Tasks.GetMergedTask<RemoveFromIndexTask>(idsToSkip, allIndexes);
                    Assert.NotNull(task1);

                    task1 = actions.Tasks.GetMergedTask<RemoveFromIndexTask>(idsToSkip, allIndexes);
                    Assert.Null(task1);

                    var task2 = actions.Tasks.GetMergedTask<TouchReferenceDocumentIfChangedTask>(idsToSkip, allIndexes);
                    Assert.NotNull(task2);

                    task2 = actions.Tasks.GetMergedTask<TouchReferenceDocumentIfChangedTask>(idsToSkip, allIndexes);
                    Assert.Null(task2);
                });

                storage.Batch(accessor =>
                {
                    Assert.False(accessor.Tasks.HasTasks);
                    Assert.Equal(0, accessor.Tasks.ApproximateTaskCount);
                });

                storage.Batch(actions =>
                {
                    var isIndexStale = actions.Staleness.IsIndexStale(100, null, null);
                    Assert.False(isIndexStale);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void returns_only_tasks_for_existing_indexes(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(actions =>
                {
                    int id = 99;
                    for (int i = 0; i < 3; i++)
                    {
                        var task = new RemoveFromIndexTask(id);
                        id++;
                        task.AddKey("tasks/" + i);
                        actions.Tasks.AddTask(task, SystemTime.UtcNow);
                    }

                    id = 99;
                    for (int i = 0; i < 3; i++)
                    {
                        var task = new TouchReferenceDocumentIfChangedTask(id);
                        id++;
                        actions.Tasks.AddTask(task, SystemTime.UtcNow);
                    }
                });

                storage.Batch(actions =>
                {
                    var idsToSkip = new List<int>();
                    var allIndexes = new[] {100};

                    var task1 = actions.Tasks.GetMergedTask<RemoveFromIndexTask>(idsToSkip, allIndexes);
                    Assert.NotNull(task1);
                    Assert.Equal(100, task1.Index);

                    task1 = actions.Tasks.GetMergedTask<RemoveFromIndexTask>(idsToSkip, allIndexes);
                    Assert.Null(task1);

                    var task2 = actions.Tasks.GetMergedTask<TouchReferenceDocumentIfChangedTask>(idsToSkip, allIndexes);
                    Assert.NotNull(task2);
                    Assert.Equal(100, task2.Index);

                    task2 = actions.Tasks.GetMergedTask<TouchReferenceDocumentIfChangedTask>(idsToSkip, allIndexes);
                    Assert.Null(task2);
                });

                storage.Batch(accessor =>
                {
                    Assert.False(accessor.Tasks.HasTasks);
                    Assert.Equal(0, accessor.Tasks.ApproximateTaskCount);
                });

                storage.Batch(actions =>
                {
                    var isIndexStale = actions.Staleness.IsIndexStale(100, null, null);
                    Assert.False(isIndexStale);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void returns_only_tasks_for_existing_indexes_with_merging(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(actions =>
                {
                    int id = 100;
                    for (int i = 0; i < 3; i++)
                    {
                        var task = new RemoveFromIndexTask(id);
                        id++;
                        task.AddKey("tasks/" + i);
                        actions.Tasks.AddTask(task, SystemTime.UtcNow);

                        task = new RemoveFromIndexTask(id);
                        id++;
                        task.AddKey("tasks/" + i + 1);
                        actions.Tasks.AddTask(task, SystemTime.UtcNow);
                    }

                    id = 100;
                    for (int i = 0; i < 3; i++)
                    {
                        var task = new TouchReferenceDocumentIfChangedTask(id);
                        id++;
                        actions.Tasks.AddTask(task, SystemTime.UtcNow);

                        task = new TouchReferenceDocumentIfChangedTask(id);
                        id++;
                        actions.Tasks.AddTask(task, SystemTime.UtcNow);
                    }
                });

                storage.Batch(actions =>
                {
                    var idsToSkip = new List<int>();
                    var allIndexes = new[] { 100 };

                    var task1 = actions.Tasks.GetMergedTask<RemoveFromIndexTask>(idsToSkip, allIndexes);
                    Assert.NotNull(task1);
                    Assert.Equal(100, task1.Index);

                    task1 = actions.Tasks.GetMergedTask<RemoveFromIndexTask>(idsToSkip, allIndexes);
                    Assert.Null(task1);

                    var task2 = actions.Tasks.GetMergedTask<TouchReferenceDocumentIfChangedTask>(idsToSkip, allIndexes);
                    Assert.NotNull(task2);
                    Assert.Equal(100, task2.Index);

                    task2 = actions.Tasks.GetMergedTask<TouchReferenceDocumentIfChangedTask>(idsToSkip, allIndexes);
                    Assert.Null(task2);
                });

                storage.Batch(accessor =>
                {
                    Assert.False(accessor.Tasks.HasTasks);
                    Assert.Equal(0, accessor.Tasks.ApproximateTaskCount);
                });

                storage.Batch(actions =>
                {
                    var isIndexStale = actions.Staleness.IsIndexStale(100, null, null);
                    Assert.False(isIndexStale);
                });
            }
        }

        [Fact]
        public void CanGetNumberOfKeysFromRemoveTask()
        {
            var task1 = new RemoveFromIndexTask(101);
            Assert.Equal(0, task1.NumberOfKeys);

            for (var i = 0; i < 100; i++)
            {
                task1.AddKey("key/" + i);
            }
            Assert.Equal(100, task1.NumberOfKeys);

            var task2 = new RemoveFromIndexTask(102);
            task2.AddKey("test1");
            task2.AddKey("test2");

            task1.Merge(task2);
            Assert.Equal(102, task1.NumberOfKeys);

            var task3 = new RemoveFromIndexTask(103);
            task2.AddKey("test2");

            task1.Merge(task3);
            Assert.Equal(102, task1.NumberOfKeys);
        }

        [Fact]
        public void CanGetNumberOfKeysFromTouchReferenceTask()
        {
            var task1 = new TouchReferenceDocumentIfChangedTask(101);
            Assert.Equal(0, task1.NumberOfKeys);

            for (var i = 0; i < 100; i++)
            {
                task1.UpdateReferenceToCheck(new KeyValuePair<string, Etag>("key/" + i, Etag.Empty));
            }
            Assert.Equal(100, task1.NumberOfKeys);

            var task2 = new TouchReferenceDocumentIfChangedTask(102);
            task2.UpdateReferenceToCheck(new KeyValuePair<string, Etag>("test1", Etag.Empty));
            task2.UpdateReferenceToCheck(new KeyValuePair<string, Etag>("test2", Etag.Empty));
            task1.Merge(task2);
            Assert.Equal(102, task1.NumberOfKeys);

            var task3 = new TouchReferenceDocumentIfChangedTask(103);
            task3.UpdateReferenceToCheck(new KeyValuePair<string, Etag>("test1", Etag.Empty.IncrementBy(2)));
            task3.UpdateReferenceToCheck(new KeyValuePair<string, Etag>("test2", Etag.Empty.IncrementBy(1)));
            task1.Merge(task3);
            Assert.Equal(102, task1.NumberOfKeys);
        }
    }
}