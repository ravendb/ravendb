using System;

using Raven.Abstractions;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Tasks;
using Raven.Server.Json;

using Xunit;

namespace FastTests.Server.Documents.Tasks
{
    public class TasksStorageTests
    {
        [Fact]
        public void SimpleTask()
        {
            using (var database = CreateDocumentDatabase())
            using (var tasksStorage = new TasksStorage())
            using (var context = new RavenOperationContext(new UnmanagedBuffersPool(string.Empty), database.DocumentsStorage.Environment))
            {
                using (var tx = context.OpenWriteTransaction())
                {
                    tasksStorage.AddTask(context, new RemoveFromIndexTask(101), DateTime.Now);

                    tx.Commit();
                }

                using (var tx = context.OpenWriteTransaction())
                {
                    var task = tasksStorage.GetMergedTask(context, 101, DocumentsTask.DocumentsTaskType.RemoveFromIndex);
                    Assert.NotNull(task);

                    tx.Commit();
                }

                using (context.OpenReadTransaction())
                {
                    var task = tasksStorage.GetMergedTask(context, 101, DocumentsTask.DocumentsTaskType.RemoveFromIndex);
                    Assert.Null(task);
                }
            }
        }

        [Fact]
        public void MergingTask()
        {
            using (var database = CreateDocumentDatabase())
            using (var tasksStorage = new TasksStorage())
            using (var context = new RavenOperationContext(new UnmanagedBuffersPool(string.Empty), database.DocumentsStorage.Environment))
            {
                using (var tx = context.OpenWriteTransaction())
                {
                    tasksStorage.AddTask(context, new RemoveFromIndexTask(101), DateTime.Now);

                    tx.Commit();
                }

                using (var tx = context.OpenWriteTransaction())
                {
                    tasksStorage.AddTask(context, new RemoveFromIndexTask(101), DateTime.Now);

                    tx.Commit();
                }

                using (var tx = context.OpenWriteTransaction())
                {
                    var task = tasksStorage.GetMergedTask(context, 101, DocumentsTask.DocumentsTaskType.RemoveFromIndex);
                    Assert.NotNull(task);

                    tx.Commit();
                }

                using (context.OpenReadTransaction())
                {
                    var task = tasksStorage.GetMergedTask(context, 101, DocumentsTask.DocumentsTaskType.RemoveFromIndex);
                    Assert.Null(task);
                }
            }
        }

        [Fact]
        public void CanAddAndRemoveMultipleTasks_InSingleTx()
        {
            using (var database = CreateDocumentDatabase())
            using (var tasksStorage = new TasksStorage())
            using (var context = new RavenOperationContext(new UnmanagedBuffersPool(string.Empty), database.DocumentsStorage.Environment))
            {
                using (var tx = context.OpenWriteTransaction())
                {
                    for (int i = 0; i < 3; i++)
                    {
                        var task = new RemoveFromIndexTask(100);
                        task.AddKey("tasks/" + i);
                        tasksStorage.AddTask(context, task, SystemTime.UtcNow);
                    }

                    tx.Commit();
                }

                using (var tx = context.OpenWriteTransaction())
                {
                    var task = tasksStorage.GetMergedTask(context, 100, DocumentsTask.DocumentsTaskType.RemoveFromIndex);
                    Assert.NotNull(task);

                    task = tasksStorage.GetMergedTask(context, 100, DocumentsTask.DocumentsTaskType.RemoveFromIndex);
                    Assert.Null(task);

                    tx.Commit();
                }

                using (context.OpenReadTransaction())
                {
                    var task = tasksStorage.GetMergedTask(context, 101, DocumentsTask.DocumentsTaskType.RemoveFromIndex);
                    Assert.Null(task);
                }
            }
        }

        [Fact]
        public void CanAddAndRemoveMultipleTasks_InSingleTx_OneByOne()
        {
            throw new NotSupportedException("Should we support this?");
        }

        [Fact]
        public void DontRemoveTasksWhenReachingMaxTaskId()
        {
            throw new NotSupportedException("Should we support this?");
        }

        [Fact]
        public void CanUpdateMaxTaskId()
        {
            throw new NotSupportedException("Should we support this?");
        }

        [Fact]
        public void MaxTaskIdIsntUpdatedWhenThereAreNoTasks()
        {
            throw new NotSupportedException("Should we support this?");
        }

        [Fact]
        public void CorrectlyNotifyAboutWorkAfterReachingMaxTaskId()
        {
            throw new NotSupportedException("Should we support this?");
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
            throw new NotImplementedException();
        }

        private static DocumentDatabase CreateDocumentDatabase()
        {
            var documentDatabase = new DocumentDatabase("Test", new RavenConfiguration { Core = { RunInMemory = true } });
            documentDatabase.Initialize();

            return documentDatabase;
        }
    }
}