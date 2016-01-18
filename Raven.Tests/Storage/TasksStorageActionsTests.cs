// -----------------------------------------------------------------------
//  <copyright file="TasksStorageActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
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
                    var foundWork = new Reference<bool>();
                    var task = accessor.Tasks.GetMergedTask<RemoveFromIndexTask>(
                        x => MaxTaskIdStatus.Updated,
                        x => { },
                        foundWork,
                        new List<int>());
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
                    var foundWork = new Reference<bool>();
                    var task = accessor.Tasks.GetMergedTask<RemoveFromIndexTask>(
                        x => MaxTaskIdStatus.Updated,
                        x => { },
                        foundWork,
                        new List<int>());
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
                    var foundWork = new Reference<bool>();
                    var task = actions.Tasks.GetMergedTask<RemoveFromIndexTask>(
                        x => MaxTaskIdStatus.Updated,
                        x => { },
                        foundWork,
                        new List<int>());
                    Assert.NotNull(task);
                    Assert.False(foundWork.Value);

                    foundWork = new Reference<bool>();
                    task = actions.Tasks.GetMergedTask<RemoveFromIndexTask>(
                        x => MaxTaskIdStatus.Updated,
                        x => { },
                        foundWork,
                        new List<int>());
                    Assert.Null(task);
                    Assert.False(foundWork.Value);
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
        public void CanAddAndRemoveMultipleTasks_InSingleTx_OneByOne(string requestedStorage)
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
                    Reference<bool> foundWork;
                    DatabaseTask task;
                    for (int i = 0; i < 3; i++)
                    {
                        foundWork = new Reference<bool>();
                        task = actions.Tasks.GetMergedTask<RemoveFromIndexTask>(
                            x => MaxTaskIdStatus.MergeDisabled,
                            x => { },
                            foundWork,
                            new List<int>());
                        Assert.NotNull(task);
                        Assert.False(foundWork.Value);
                    }

                    foundWork = new Reference<bool>();
                    task = actions.Tasks.GetMergedTask<RemoveFromIndexTask>(
                        x => MaxTaskIdStatus.Updated,
                        x => { },
                        foundWork,
                        new List<int>());
                    Assert.Null(task);
                    Assert.False(foundWork.Value);
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
        public void DontRemoveTasksWhenReachingMaxTaskId(string requestedStorage)
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
                    var foundWork = new Reference<bool>();
                    var task = actions.Tasks.GetMergedTask<RemoveFromIndexTask>(
                        x => MaxTaskIdStatus.ReachedMaxTaskId,
                        x => { },
                        foundWork,
                        new List<int>());
                    Assert.Null(task);
                    Assert.True(foundWork.Value);

                    for (int i = 0; i < 3; i++)
                    {
                        foundWork = new Reference<bool>();
                        task = actions.Tasks.GetMergedTask<RemoveFromIndexTask>(
                            x => MaxTaskIdStatus.MergeDisabled,
                            x => { },
                            foundWork,
                            new List<int>());
                        Assert.NotNull(task);
                        Assert.False(foundWork.Value);
                    }

                    task = actions.Tasks.GetMergedTask<RemoveFromIndexTask>(
                        x => MaxTaskIdStatus.MergeDisabled,
                        x => { },
                        foundWork,
                        new List<int>());
                    Assert.Null(task);
                    Assert.False(foundWork.Value);
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
        public void CanUpdateMaxTaskId(string requestedStorage)
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
                    IComparable maxTaskId = null;
                    var foundWork = new Reference<bool>();
                    var task = actions.Tasks.GetMergedTask<RemoveFromIndexTask>(
                        x => MaxTaskIdStatus.Updated,
                        x => maxTaskId = x,
                        foundWork,
                        new List<int>());
                    Assert.NotNull(task);
                    Assert.NotNull(maxTaskId);
                    Assert.False(foundWork.Value);

                    maxTaskId = null;
                    foundWork = new Reference<bool>();
                    task = actions.Tasks.GetMergedTask<RemoveFromIndexTask>(
                        x => MaxTaskIdStatus.Updated,
                        x => maxTaskId = x,
                        foundWork,
                        new List<int>());
                    Assert.Null(task);
                    Assert.Null(maxTaskId);
                    Assert.False(foundWork.Value);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void MaxTaskIdIsntUpdatedWhenThereAreNoTasks(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(actions =>
                {
                    IComparable maxTaskId = null;
                    var foundWork = new Reference<bool>();
                    var task = actions.Tasks.GetMergedTask<RemoveFromIndexTask>(
                        x => MaxTaskIdStatus.Updated,
                        x => maxTaskId = x,
                        foundWork,
                        new List<int>());
                    Assert.Null(task);
                    Assert.Null(maxTaskId);
                    Assert.False(foundWork.Value);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void CorrectlyNotifyAboutWorkAfterReachingMaxTaskId(string requestedStorage)
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
                    var foundWork = new Reference<bool>();
                    var task = actions.Tasks.GetMergedTask<RemoveFromIndexTask>(
                        x => MaxTaskIdStatus.ReachedMaxTaskId,
                        x => { },
                        foundWork,
                        new List<int>());
                    Assert.Null(task);
                    Assert.True(foundWork.Value);

                    for (int i = 0; i < 3; i++)
                    {
                        foundWork = new Reference<bool>();
                        task = actions.Tasks.GetMergedTask<RemoveFromIndexTask>(
                            x => MaxTaskIdStatus.MergeDisabled,
                            x => { },
                            foundWork,
                            new List<int>());
                        Assert.NotNull(task);
                        Assert.False(foundWork.Value);
                    }

                    task = actions.Tasks.GetMergedTask<RemoveFromIndexTask>(
                        x => MaxTaskIdStatus.Updated,
                        x => { },
                        foundWork,
                        new List<int>());
                    Assert.Null(task);
                    Assert.False(foundWork.Value);
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