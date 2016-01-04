//-----------------------------------------------------------------------
// <copyright file="Tasks.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Tasks;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Storage
{
    public class Tasks : RavenTest
    {
        int test = 100;
        [Fact]
        public void CanCheckForExistenceOfTasks()
        {
            using (var tx = NewTransactionalStorage())
            {
                tx.Batch(accessor => accessor.Indexing.AddIndex(test, false));
                tx.Batch(viewer => Assert.False(viewer.Staleness.IsIndexStale(test, null, null)));
                tx.Batch(mutator => mutator.Tasks.AddTask(new RemoveFromIndexTask(test), SystemTime.UtcNow));
                tx.Batch(viewer => Assert.True(viewer.Staleness.IsIndexStale(test, null, null)));
            }
        }

        [Fact]
        public void CanCheckForExistenceOfTasksAfterTaskWasRemoved()
        {
            using (var tx = NewTransactionalStorage())
            {
                tx.Batch(accessor => accessor.Indexing.AddIndex(test, false));
                tx.Batch(viewer => Assert.False(viewer.Staleness.IsIndexStale(test, null, null)));
                tx.Batch(mutator => mutator.Tasks.AddTask(new RemoveFromIndexTask(test), SystemTime.UtcNow));
                tx.Batch(viewer => Assert.True(viewer.Staleness.IsIndexStale(test, null, null)));

                tx.Batch(mutator => Assert.NotNull(mutator.Tasks.GetMergedTask<RemoveFromIndexTask>(
                    x => MaxTaskIdStatus.Updated,
                    x => { },
                    new Reference<bool>()
                )));
                tx.Batch(viewer => Assert.False(viewer.Staleness.IsIndexStale(test, null, null)));
            }
        }


        [Fact]
        public void CanCheckForExistenceOfTasksWithCutOffs()
        {
            using (var tx = NewTransactionalStorage())
            {
                var cutoff = SystemTime.UtcNow;
                tx.Batch(mutator =>
                {
                    mutator.Indexing.AddIndex(test, false);
                    mutator.Indexing.UpdateLastIndexed(test, Etag.InvalidEtag, cutoff);
                });
                tx.Batch(mutator => mutator.Tasks.AddTask(new RemoveFromIndexTask(test), SystemTime.UtcNow));
                tx.Batch(viewer => Assert.True(viewer.Staleness.IsIndexStale(test, null, null)));
                tx.Batch(viewer => Assert.True(viewer.Staleness.IsIndexStale(test, cutoff.AddMinutes(1), null)));
                tx.Batch(viewer => Assert.False(viewer.Staleness.IsIndexStale(test, cutoff.AddMinutes(-1), null)));
            }
        }

        [Fact]
        public void CanGetTask()
        {
            using (var tx = NewTransactionalStorage())
            {
                tx.Batch(mutator => mutator.Tasks.AddTask(new RemoveFromIndexTask(test), SystemTime.UtcNow));
                tx.Batch(mutator => Assert.NotNull(mutator.Tasks.GetMergedTask<RemoveFromIndexTask>(
                    x => MaxTaskIdStatus.Updated,
                    x => { },
                    new Reference<bool>()
                )));
            }
        }

        [Fact]
        public void AfterGettingTaskOnceWillNotGetItAgain()
        {
            using (var tx = NewTransactionalStorage())
            {
                tx.Batch(mutator => mutator.Tasks.AddTask(new RemoveFromIndexTask(test), SystemTime.UtcNow));
                tx.Batch(mutator => Assert.NotNull(mutator.Tasks.GetMergedTask<RemoveFromIndexTask>(
                    x => MaxTaskIdStatus.Updated,
                    x => { },
                    new Reference<bool>()
                )));
                tx.Batch(mutator => Assert.Null(mutator.Tasks.GetMergedTask<RemoveFromIndexTask>(
                    x => MaxTaskIdStatus.Updated,
                    x => { },
                    new Reference<bool>()
                )));
            }
        }

        [Fact]
        public void CanMergeTasks()
        {
            using (var tx = NewTransactionalStorage())
            {
                tx.Batch(mutator =>
                {
                    var removeFromIndexTask = new RemoveFromIndexTask(test);
                    removeFromIndexTask.AddKey("a");
                    mutator.Tasks.AddTask(removeFromIndexTask, SystemTime.UtcNow);
                });
                tx.Batch(mutator =>
                {
                    var removeFromIndexTask = new RemoveFromIndexTask(test);
                    removeFromIndexTask.AddKey("b");
                    mutator.Tasks.AddTask(removeFromIndexTask, SystemTime.UtcNow);
                });
                var foundWork = new Reference<bool>();
                tx.Batch(mutator => Assert.Equal(2, mutator.Tasks.GetMergedTask<RemoveFromIndexTask>(
                    x => MaxTaskIdStatus.Updated,
                    x => { },
                    foundWork
                ).NumberOfKeys));
            }
        }
    }
}
