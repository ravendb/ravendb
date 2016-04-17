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
                    var alreadySeen = new HashSet<IComparable>();
                    var task = accessor.Tasks.GetMergedTask<RemoveFromIndexTask>(new List<int>(), new[] {101}, alreadySeen);
                    Assert.NotNull(task);
                    accessor.Tasks.DeleteTasks(alreadySeen);
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
                    var alreadySeen = new HashSet<IComparable>();
                    var task = accessor.Tasks.GetMergedTask<RemoveFromIndexTask>(new List<int>(), new[] {101}, alreadySeen);
                    Assert.NotNull(task);
                    accessor.Tasks.DeleteTasks(alreadySeen);
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
                    var alreadySeen = new HashSet<IComparable>();
                    var task = actions.Tasks.GetMergedTask<RemoveFromIndexTask>(new List<int>(), new[] {100}, alreadySeen);
                    Assert.NotNull(task);
                    actions.Tasks.DeleteTasks(alreadySeen);

                    task = actions.Tasks.GetMergedTask<RemoveFromIndexTask>(new List<int>(), new[] {100}, new HashSet<IComparable>());
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
                    var alreadySeen = new HashSet<IComparable>();

                    var task1 = actions.Tasks.GetMergedTask<RemoveFromIndexTask>(idsToSkip, allIndexes, alreadySeen);
                    Assert.NotNull(task1);

                    task1 = actions.Tasks.GetMergedTask<RemoveFromIndexTask>(idsToSkip, allIndexes, alreadySeen);
                    Assert.Null(task1);

                    var task2 = actions.Tasks.GetMergedTask<TouchReferenceDocumentIfChangedTask>(idsToSkip, allIndexes, alreadySeen);
                    Assert.NotNull(task2);

                    task2 = actions.Tasks.GetMergedTask<TouchReferenceDocumentIfChangedTask>(idsToSkip, allIndexes, alreadySeen);
                    Assert.Null(task2);

                    actions.Tasks.DeleteTasks(alreadySeen);
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
                    var alreadySeen = new HashSet<IComparable>();

                    var task1 = actions.Tasks.GetMergedTask<RemoveFromIndexTask>(idsToSkip, allIndexes, alreadySeen);
                    Assert.Equal(1, task1.NumberOfKeys);
                    Assert.NotNull(task1);
                    Assert.Equal(100, task1.Index);
                    actions.Tasks.DeleteTasks(alreadySeen);

                    task1 = actions.Tasks.GetMergedTask<RemoveFromIndexTask>(idsToSkip, allIndexes, alreadySeen);
                    Assert.Null(task1);

                    var task2 = actions.Tasks.GetMergedTask<TouchReferenceDocumentIfChangedTask>(idsToSkip, allIndexes, alreadySeen);
                    Assert.NotNull(task2);
                    Assert.Equal(false, task2.SeparateTasksByIndex);
                    Assert.Equal(100, task2.Index);
                    actions.Tasks.DeleteTasks(alreadySeen);

                    task2 = actions.Tasks.GetMergedTask<TouchReferenceDocumentIfChangedTask>(idsToSkip, allIndexes, alreadySeen);
                    Assert.Null(task2);
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
                    var alreadySeen = new HashSet<IComparable>();

                    var task1 = actions.Tasks.GetMergedTask<RemoveFromIndexTask>(idsToSkip, allIndexes, alreadySeen);
                    Assert.Equal(1, task1.NumberOfKeys);
                    Assert.NotNull(task1);
                    Assert.Equal(100, task1.Index);
                    actions.Tasks.DeleteTasks(alreadySeen);

                    task1 = actions.Tasks.GetMergedTask<RemoveFromIndexTask>(idsToSkip, allIndexes, alreadySeen);
                    Assert.Null(task1);

                    var task2 = actions.Tasks.GetMergedTask<TouchReferenceDocumentIfChangedTask>(idsToSkip, allIndexes, alreadySeen);
                    Assert.NotNull(task2);
                    Assert.Equal(false, task2.SeparateTasksByIndex);
                    Assert.Equal(100, task2.Index);
                    actions.Tasks.DeleteTasks(alreadySeen);

                    task2 = actions.Tasks.GetMergedTask<TouchReferenceDocumentIfChangedTask>(idsToSkip, allIndexes, alreadySeen);
                    Assert.Null(task2);
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
        public void returns_only_working_indexes(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(actions =>
                {
                    var id = 100;
                    for (int i = 0; i < 3; i++)
                    {
                        var task = new TouchReferenceDocumentIfChangedTask(id);
                        task.UpdateReferenceToCheck(new KeyValuePair<string, Etag>(i.ToString(), Etag.Empty));
                        id++;
                        actions.Tasks.AddTask(task, SystemTime.UtcNow);
                    }
                });

                storage.Batch(actions =>
                {
                    var idsToSkip = new List<int>() { 101 };
                    var allIndexes = new[] { 100 };
                    var alreadySeen = new HashSet<IComparable>();

                    var task1 = actions.Tasks.GetMergedTask<TouchReferenceDocumentIfChangedTask>(idsToSkip, allIndexes, alreadySeen);
                    Assert.Equal(1, task1.NumberOfKeys);
                    Assert.NotNull(task1);
                    Assert.Equal(100, task1.Index);
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
        public void can_remove_all_tasks_for_one_index(string requestedStorage)
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
                    Assert.Equal(3, actions.Tasks.ApproximateTaskCount);
                    actions.Tasks.DeleteTasksForIndex(100);
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
        public void can_remove_two_tasks_for_one_index(string requestedStorage)
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
                        var task = new RemoveFromIndexTask(101);
                        task.AddKey("tasks/" + i);
                        actions.Tasks.AddTask(task, SystemTime.UtcNow);
                    }
                });

                storage.Batch(actions =>
                {
                    Assert.Equal(6, actions.Tasks.ApproximateTaskCount);
                    actions.Tasks.DeleteTasksForIndex(100);
                });

                storage.Batch(accessor =>
                {
                    Assert.True(accessor.Tasks.HasTasks);
                    Assert.Equal(3, accessor.Tasks.ApproximateTaskCount);
                });

                storage.Batch(actions =>
                {
                    var isIndexStale = actions.Staleness.IsIndexStaleByTask(100, null);
                    Assert.False(isIndexStale);

                    isIndexStale = actions.Staleness.IsIndexStaleByTask(101, null);
                    Assert.True(isIndexStale);
                });

                storage.Batch(actions => actions.Tasks.DeleteTasksForIndex(101));

                storage.Batch(accessor =>
                {
                    Assert.False(accessor.Tasks.HasTasks);
                    Assert.Equal(0, accessor.Tasks.ApproximateTaskCount);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void can_remove_two_different_type_tasks_for_one_index(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(actions =>
                {
                    for (int i = 0; i < 3; i++)
                    {
                        var task1 = new RemoveFromIndexTask(100);
                        task1.AddKey("tasks/" + i);
                        actions.Tasks.AddTask(task1, SystemTime.UtcNow);

                        var task2 = new TouchReferenceDocumentIfChangedTask(101);
                        actions.Tasks.AddTask(task2, SystemTime.UtcNow);
                    }
                });

                storage.Batch(actions =>
                {
                    Assert.Equal(6, actions.Tasks.ApproximateTaskCount);
                    actions.Tasks.DeleteTasksForIndex(100);
                });

                storage.Batch(accessor => Assert.True(accessor.Tasks.HasTasks));

                storage.Batch(actions =>
                {
                    var isIndexStale = actions.Staleness.IsIndexStaleByTask(100, null);
                    Assert.False(isIndexStale);

                    isIndexStale = actions.Staleness.IsIndexStaleByTask(101, null);
                    Assert.True(isIndexStale);
                });

                storage.Batch(actions => actions.Tasks.DeleteTasksForIndex(101));

                storage.Batch(accessor =>
                {
                    Assert.False(accessor.Tasks.HasTasks);
                    Assert.Equal(0, accessor.Tasks.ApproximateTaskCount);

                    var isIndexStale = accessor.Staleness.IsIndexStaleByTask(101, null);
                    Assert.False(isIndexStale);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void can_remove_different_type_tasks_for_one_index(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(actions =>
                {
                    for (int i = 0; i < 3; i++)
                    {
                        var task1 = new RemoveFromIndexTask(100);
                        task1.AddKey("tasks/" + i);
                        actions.Tasks.AddTask(task1, SystemTime.UtcNow);

                        var task2 = new TouchReferenceDocumentIfChangedTask(100);
                        actions.Tasks.AddTask(task2, SystemTime.UtcNow);
                    }
                });

                storage.Batch(actions =>
                {
                    Assert.Equal(6, actions.Tasks.ApproximateTaskCount);
                    actions.Tasks.DeleteTasksForIndex(101);
                });

                storage.Batch(accessor =>
                {
                    Assert.True(accessor.Tasks.HasTasks);
                    Assert.Equal(6, accessor.Tasks.ApproximateTaskCount);
                });

                storage.Batch(actions =>
                {
                    var isIndexStale = actions.Staleness.IsIndexStale(100, null, null);
                    Assert.False(isIndexStale);
                });

                storage.Batch(actions => actions.Tasks.DeleteTasksForIndex(100));

                storage.Batch(accessor =>
                {
                    Assert.False(accessor.Tasks.HasTasks);
                    Assert.Equal(0, accessor.Tasks.ApproximateTaskCount);

                    var isIndexStale = accessor.Staleness.IsIndexStale(101, null, null);
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