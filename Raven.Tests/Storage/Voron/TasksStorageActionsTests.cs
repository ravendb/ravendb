// -----------------------------------------------------------------------
//  <copyright file="TasksStorageActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Tests.Common;

namespace Raven.Tests.Storage.Voron
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
                        foundWork);
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
                        x => {},
                        foundWork);
                    Assert.NotNull(task);
                });

                storage.Batch(accessor =>
                {
                    Assert.False(accessor.Tasks.HasTasks);
                    Assert.Equal(0, accessor.Tasks.ApproximateTaskCount);
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
