using System;
using System.Data;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Voron;
using Voron;
using Xunit;
using TimeoutException = System.TimeoutException;
using LockTaskResponsible = Voron.Impl.Journal.WriteAheadJournal.JournalApplicator.LockTaskResponsible;
using SyncOperation = Voron.Impl.Journal.WriteAheadJournal.JournalApplicator.SyncOperation;

namespace SlowTests.Voron.Storage
{
    public class SyncFlushTimingTest : StorageTest
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
            options.SyncDisabled = true;
        }

        [Fact]
        // As part of the sync operation, there are stages where the sync operation needs the flush lock
        // and as part of the flush operation, there are stages the flush operation needs transaction write lock
        // this can lead to a situation where the sync is waiting to flush waiting to write transaction
        // so the sync pass his work that needs the flush lock to the flush operation if the lock is occupied 
        // and if the flush operation can do it while it waits to write transaction lock

        //In this test, the sync is called while the flush is running and waiting to write transaction so the sync should not be blocked 
        public void CanSyncWhileFlushWaiteToWriteTransaction()
        {
            var syncMayFinishedEvent = new AutoResetEvent(false);

            //Adding unsynced bytes so the sync thread will has work to do
            for (var i = 0; i < 100; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("foo");
                    tree.Add("items/" + i, StreamFor("values/" + i));
                    tx.Commit();
                }
            }
            Env.FlushLogToDataFile();

            //Adding unflushed bytes so the flush thread will has work to do
            for (var i = 0; i < 100; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("foo");
                    tree.Add("items/" + i, StreamFor("values/" + i));
                    tx.Commit();
                }
            }

            var syncResult = false;

            void Sync()
            {
                try
                {
                    using (var operation = new SyncOperation(Env.Journal.Applicator))
                    {
                        syncResult = operation.SyncDataFile();
                    }
                }
                finally
                {
                    syncMayFinishedEvent.Set();
                }
            }

            void Flush()
            {
                try
                {
                    using (Env.Journal.Applicator.TakeFlushingLock())
                    {
                        Task.Run((Action)Sync);
                        Env.FlushLogToDataFile();
                    }
                }
                catch (Exception)
                {
                    syncMayFinishedEvent.Set();
                }
            }

            // Write transaction lock is taken to block the flush
            using (var tx = Env.WriteTransaction())
            {
                Task.Run((Action)Flush);

                syncMayFinishedEvent.WaitOne(TimeSpan.FromSeconds(10));
                Assert.True(syncResult);
                var totalWrittenButUnsyncedBytes = Env.Journal.Applicator.TotalWrittenButUnsyncedBytes;
                Assert.Equal(0, totalWrittenButUnsyncedBytes);
            }
        }

        [Fact(Timeout = 10 * 1000)]
        // http://issues.hibernatingrhinos.com/issue/RavenDB-12525
        // The problem was when sync operates while there is no data to sync
        // the sync operation continued with default properties
        // and at the end, it updated the state with those properties
        public void Sync_WhenThereIsNoJournalToSync_ShouldNotUpdateHeaderToDefault()
        {
            for (var i = 0; i < 100; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("foo");
                    tree.Add("items/" + i, StreamFor("values/" + i));
                    tx.Commit();
                }
            }
            Env.FlushLogToDataFile();

            using (var operation = new SyncOperation(Env.Journal.Applicator))
            {
                operation.SyncDataFile();
            }

            using (var operation = new SyncOperation(Env.Journal.Applicator))
            {
                operation.SyncDataFile();
            }

            var journalInfo = Env.Journal.GetCurrentJournalInfo();

            Assert.NotEqual(-1, journalInfo.LastSyncedJournal);
            Assert.NotEqual(-1, journalInfo.LastSyncedTransactionId);
        }

        [Fact]
        public void LockTaskResponsible_WhenLockTakenAndNoOneRunTaskIfNotAlreadyRan_ShouldKeepWaiting()
        {
            var tokenSource = new CancellationTokenSource();
            var worker = new Worker("worker");
            try
            {
                var @lock = new object();
                var lockTaskResponsible = new LockTaskResponsible(@lock, tokenSource.Token);
                Monitor.Enter(@lock);

                worker.TaskRun(j => lockTaskResponsible.WaitForTaskToBeDone(j));

                Assert.Throws<TimeoutException>(() => worker.WaitThrow(TimeSpan.FromSeconds(5)));

                Assert.Equal(0, worker.Job.TimesJobDone);
                Assert.Equal(null, worker.Exception);
            }
            finally
            {
                tokenSource.Cancel();
            }
        }

        [Fact]
        public void LockTaskResponsible_WhenLockTakenAndNoOneRunTaskIfNotAlreadyRanAndCancelToken_ShouldContinueWithoutDoJob()
        {
            var tokenSource = new CancellationTokenSource();
            var worker = new Worker("worker");

            var @lock = new object();
            var lockTaskResponsible = new LockTaskResponsible(@lock, tokenSource.Token);
            Monitor.Enter(@lock);

            worker.TaskRun(j => lockTaskResponsible.WaitForTaskToBeDone(j));
            tokenSource.Cancel();

            worker.WaitThrow(TimeSpan.FromSeconds(10));
            Assert.Equal(0, worker.Job.TimesJobDone);
            Assert.Equal(null, worker.Exception);
        }

        [Fact]
        public void LockTaskResponsible_WhenLockTakenAndRelease_JobShouldBeDone()
        {
            var tokenSource = new CancellationTokenSource();
            try
            {
                var worker = new Worker("worker");

                var @lock = new object();
                var lockTaskResponsible = new LockTaskResponsible(@lock, tokenSource.Token);
                Monitor.Enter(@lock);

                worker.TaskRun((j) => lockTaskResponsible.WaitForTaskToBeDone(j));

                Thread.Sleep(TimeSpan.FromSeconds(1));
                Monitor.Exit(@lock);

                worker.WaitThrow(TimeSpan.FromSeconds(10));
                Assert.Equal(1, worker.Job.TimesJobDone);
                Assert.Equal(null, worker.Exception);
                Assert.True(worker.Result);
            }
            catch (Exception)
            {
                tokenSource.Cancel();
                throw;
            }
        }

        [Fact]
        public void LockTaskResponsible_WhenLockTaken_ShouldBeDoneByRunTaskIfNotAlreadyRan()
        {
            var tokenSource = new CancellationTokenSource();
            try
            {
                var worker = new Worker("worker");

                var @lock = new object();
                var lockTaskResponsible = new LockTaskResponsible(@lock, tokenSource.Token);
                Monitor.Enter(@lock);

                worker.TaskRun((j) => lockTaskResponsible.WaitForTaskToBeDone(j));

                var stop = Stopwatch.StartNew();
                while (worker.Job.TimesJobDone == 0 && stop.Elapsed < TimeSpan.FromSeconds(10))
                {
                    lockTaskResponsible.RunTaskIfNotAlreadyRan();
                    Thread.Sleep(100);
                }

                Assert.Equal(1, worker.Job.TimesJobDone);
                Assert.Equal(null, worker.Exception);
                Assert.True(worker.Result);
            }
            catch (Exception)
            {
                tokenSource.Cancel();
                throw;
            }
        }

        [Fact]
        public void LockTaskResponsible_WhenLockTakenAndJobFailed_ShouldBeDoneByRunTaskIfNotAlreadyRanAndReturnFalse()
        {
            var tokenSource = new CancellationTokenSource();
            try
            {
                var worker = new Worker("worker")
                {
                    Job = new FailJob()
                };

                var @lock = new object();
                var lockTaskResponsible = new LockTaskResponsible(@lock, tokenSource.Token);
                Monitor.Enter(@lock);

                worker.TaskRun((j) => lockTaskResponsible.WaitForTaskToBeDone(j));

                var stop = Stopwatch.StartNew();
                while (worker.Job.TimesJobDone == 0 && stop.Elapsed < TimeSpan.FromSeconds(10))
                {
                    lockTaskResponsible.RunTaskIfNotAlreadyRan();
                    Thread.Sleep(100);
                }

                Assert.Equal(1, worker.Job.TimesJobDone);
                Assert.Equal(null, worker.Exception);
                Assert.False(worker.Result);
            }
            catch (Exception)
            {
                tokenSource.Cancel();
                throw;
            }
        }

        [Fact]
        public void LockTaskResponsible_WhenLockTakenAndDoneByRunTaskIfNotAlreadyRan_ShouldBeWaitUntilTheJobDone()
        {
            var tokenSource = new CancellationTokenSource();
            try
            {
                var worker = new Worker("worker")
                {
                    Job = new LongJob()
                };

                var @lock = new object();
                var lockTaskResponsible = new LockTaskResponsible(@lock, tokenSource.Token);
                Monitor.Enter(@lock);

                var isContinueBeforeJobFinished = true;
                worker.TaskRun(j =>
                {
                    var result = lockTaskResponsible.WaitForTaskToBeDone(j);
                    isContinueBeforeJobFinished = worker.Job.TimesJobDone == 0;
                    return result;
                });

                var stop = Stopwatch.StartNew();
                while (worker.Job.TimesJobDone == 0)
                {
                    if (stop.Elapsed > TimeSpan.FromSeconds(10))
                        throw new TimeoutException();

                    lockTaskResponsible.RunTaskIfNotAlreadyRan();
                    Thread.Sleep(100);
                }

                worker.WaitThrow(TimeSpan.FromSeconds(10));

                Assert.Equal(1, worker.Job.TimesJobDone);
                Assert.False(isContinueBeforeJobFinished);
                Assert.Equal(null, worker.Exception);
                Assert.True(worker.Result);
            }
            catch (Exception)
            {
                tokenSource.Cancel();
                throw;
            }
        }

        [Fact]
        public void LockTaskResponsible_WhenTwoThreadsAreWaitingToTaskToBeDone_TheRunThreadShouldRunOnlyOneForCallAndEventuallyCompleteAll()
        {
            var tokenSource = new CancellationTokenSource();
            try
            {
                var worker1 = new Worker("worker1");
                var worker2 = new Worker("worker2");

                var @lock = new object();
                var lockTaskResponsible = new LockTaskResponsible(@lock, tokenSource.Token);
                Monitor.Enter(@lock);

                worker1.TaskRun(j => lockTaskResponsible.WaitForTaskToBeDone(j));
                worker2.TaskRun(j => lockTaskResponsible.WaitForTaskToBeDone(j));
                Thread.Sleep(1000);

                var stop = Stopwatch.StartNew();
                while (worker1.Job.TimesJobDone == 0 || worker2.Job.TimesJobDone == 0)
                {
                    if (stop.Elapsed > TimeSpan.FromSeconds(10))
                        throw new TimeoutException();

                    lockTaskResponsible.RunTaskIfNotAlreadyRan();
                    lockTaskResponsible.RunTaskIfNotAlreadyRan();
                    Thread.Sleep(100);
                }

                worker1.WaitThrow(TimeSpan.FromSeconds(10));
                worker2.WaitThrow(TimeSpan.FromSeconds(10));

                Assert.Equal(1, worker1.Job.TimesJobDone);
                Assert.Equal(1, worker2.Job.TimesJobDone);
                Assert.Equal(null, worker1.Exception);
                Assert.Equal(null, worker2.Exception);
                Assert.True(worker1.Result);
                Assert.True(worker2.Result);
            }
            catch (Exception)
            {
                tokenSource.Cancel();
                throw;
            }
        }

        [Fact]
        public void LockTaskResponsible_WhenLockNotTaken_WorkShouldBeDone()
        {
            var tokenSource = new CancellationTokenSource();
            try
            {
                var worker = new Worker("worker");

                var @lock = new object();
                var lockTaskResponsible = new LockTaskResponsible(@lock, tokenSource.Token);

                worker.TaskRun(j => lockTaskResponsible.WaitForTaskToBeDone(j));

                worker.WaitThrow(TimeSpan.FromSeconds(10));

                Assert.Equal(1, worker.Job.TimesJobDone);
                Assert.Equal(null, worker.Exception);
                Assert.True(worker.Result);
            }
            catch (Exception)
            {
                tokenSource.Cancel();
                throw;
            }
        }

        [Fact]
        public void LockTaskResponsible_WhenLockNotTakenAndJobFailed_WorkShouldReturnFalse()
        {
            var tokenSource = new CancellationTokenSource();
            try
            {
                var worker = new Worker("worker")
                {
                    Job = new FailJob()
                };

                var @lock = new object();
                var lockTaskResponsible = new LockTaskResponsible(@lock, tokenSource.Token);

                worker.TaskRun(j => lockTaskResponsible.WaitForTaskToBeDone(j));

                worker.WaitThrow(TimeSpan.FromSeconds(10));

                Assert.Equal(1, worker.Job.TimesJobDone);
                Assert.Equal(null, worker.Exception);
                Assert.False(worker.Result);
            }
            catch (Exception)
            {
                tokenSource.Cancel();
                throw;
            }
        }

        [Fact]
        public void LockTaskResponsible_WhenLockNotTakenAndWaitTwice_WorkShouldBeDone()
        {
            var tokenSource = new CancellationTokenSource();
            try
            {
                var worker = new Worker("worker");

                var @lock = new object();
                var lockTaskResponsible = new LockTaskResponsible(@lock, tokenSource.Token);

                worker.TaskRun(j => lockTaskResponsible.WaitForTaskToBeDone(j)
                                    && lockTaskResponsible.WaitForTaskToBeDone(j));

                worker.WaitThrow(TimeSpan.FromSeconds(10));

                Assert.Equal(2, worker.Job.TimesJobDone);
                Assert.Equal(null, worker.Exception);
                Assert.True(worker.Result);
            }
            catch (Exception)
            {
                tokenSource.Cancel();
                throw;
            }
        }

        [Fact]
        public void LockTaskResponsible_WhenLockTakenAndJobThrow_ShouldThrowToTheWaiter()
        {
            var tokenSource = new CancellationTokenSource();
            try
            {
                var worker = new Worker("worker")
                {
                    Job = new ThrowJob<DataException>()
                };

                var @lock = new object();
                var lockTaskResponsible = new LockTaskResponsible(@lock, tokenSource.Token);
                Monitor.Enter(@lock);

                worker.TaskRun(j => lockTaskResponsible.WaitForTaskToBeDone(j));

                var stop = Stopwatch.StartNew();
                while (worker.Exception == null)
                {
                    if (stop.Elapsed > TimeSpan.FromSeconds(10))
                        throw new TimeoutException();

                    lockTaskResponsible.RunTaskIfNotAlreadyRan();
                    Thread.Sleep(100);
                }

                worker.WaitThrow(TimeSpan.FromSeconds(10));
                Assert.Equal(typeof(DataException), worker.Exception.InnerException.GetType());
            }
            catch (Exception)
            {
                tokenSource.Cancel();
                throw;
            }
        }

        [Fact]
        public void LockTaskResponsible_WhenTwoThreadsAreWaitingOneOfThemDoThrowJob_ShouldCompleteTheSecondJob()
        {
            var tokenSource = new CancellationTokenSource();
            try
            {
                var throwWorker = new Worker("throwWorker")
                {
                    Job = new ThrowJob<DataException>()
                };
                var worker = new Worker("worker");

                var @lock = new object();
                var lockTaskResponsible = new LockTaskResponsible(@lock, tokenSource.Token);
                Monitor.Enter(@lock);

                throwWorker.TaskRun(j => lockTaskResponsible.WaitForTaskToBeDone(j));
                Thread.Sleep(100);
                worker.TaskRun(j => lockTaskResponsible.WaitForTaskToBeDone(j));
                Thread.Sleep(100);

                var stop = Stopwatch.StartNew();
                while (throwWorker.Exception == null || worker.Job.TimesJobDone == 0)
                {
                    if (stop.Elapsed > TimeSpan.FromSeconds(10))
                        throw new TimeoutException();

                    lockTaskResponsible.RunTaskIfNotAlreadyRan();
                    lockTaskResponsible.RunTaskIfNotAlreadyRan();
                    Thread.Sleep(100);
                }

                throwWorker.WaitThrow(TimeSpan.FromSeconds(10));
                worker.WaitThrow(TimeSpan.FromSeconds(10));

                Assert.Equal(0, throwWorker.Job.TimesJobDone);
                Assert.Equal(1, worker.Job.TimesJobDone);
                Assert.Equal(typeof(DataException), throwWorker.Exception.InnerException.GetType());
                Assert.Equal(null, worker.Exception);
            }
            catch (Exception)
            {
                tokenSource.Cancel();
                throw;
            }
        }

        private class Job
        {
            private int _timesJobDone;

            public int TimesJobDone => _timesJobDone;

            public virtual bool Do()
            {
                Interlocked.Add(ref _timesJobDone, 1);
                return true;
            }
        }

        private class LongJob : Job
        {
            public override bool Do()
            {
                Thread.Sleep(TimeSpan.FromSeconds(1));
                base.Do();
                return true;
            }
        }

        private class ThrowJob<T> : Job where T : Exception
        {
            public override bool Do()
            {
                throw new DataException();
            }
        }

        private class FailJob : Job
        {
            public override bool Do()
            {
                base.Do();
                return false;
            }
        }

        private class Worker
        {
            private volatile bool _result;
            private readonly AutoResetEvent _finishedWaiting = new AutoResetEvent(false);

            public string Name { get; }
            public bool Result => _result;
            public Exception Exception { get; private set; }
            public Job Job { get; set; } = new Job();

            public Worker(string name)
            {
                Name = name;
            }

            public void TaskRun(Func<Func<bool>, bool> action)
            {
                Task.Run(() =>
                {
                    try
                    {
                        _result = action(Job.Do);
                    }
                    catch (Exception e)
                    {
                        Exception = e;
                    }
                    _finishedWaiting.Set();
                });
            }

            public void WaitThrow(TimeSpan time)
            {
                if (_finishedWaiting.WaitOne(time) == false)
                {
                    throw new TimeoutException();
                }
            }
        }
    }
}
