using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Voron;
using Voron;
using Voron.Impl.Journal;
using Xunit;

namespace SlowTests.Voron.Storage
{
    public class SyncFlushTimingTest : StorageTest
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
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

            void Sync()
            {
                try
                {
                    using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
                    {
                        operation.SyncDataFile();
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
                var totalWrittenButUnsyncedBytes = Env.Journal.Applicator.TotalWrittenButUnsyncedBytes;
                Assert.Equal(0, totalWrittenButUnsyncedBytes);
            }

        }

        [Fact]
        public void LockTaskResponsible_WhenLockTakenAndNoOneRunTaskIfNotAlreadyRan_ShouldKeepWaiting()
        {
            var job = new Job();

            var @lock = new object();
            Monitor.Enter(@lock);

            var tokenSource = new CancellationTokenSource();
            var lockTaskResponsible =
              new WriteAheadJournal.JournalApplicator.LockTaskResponsible(@lock, tokenSource.Token);

            Task.Run(() => lockTaskResponsible.WaitForTaskToBeDone(job.Do));

            job.Wait(TimeSpan.FromSeconds(5));
            Assert.Equal(0, job.TimesJobDone);
        }

        [Fact]
        public void LockTaskResponsible_WhenLockTakenAndNoOneRunTaskIfNotAlreadyRanAndCancelToken_ShouldContinueWithoutDoJob()
        {
            var job = new Job();
            var resetEvent = new ManualResetEvent(false);

            var @lock = new object();
            Monitor.Enter(@lock);

            var tokenSource = new CancellationTokenSource();
            var lockTaskResponsible =
              new WriteAheadJournal.JournalApplicator.LockTaskResponsible(@lock, tokenSource.Token);

            Task.Run(() =>
            {
                lockTaskResponsible.WaitForTaskToBeDone(job.Do);
                resetEvent.Set();
            });

            tokenSource.Cancel();

            var isContinue = resetEvent.WaitOne(TimeSpan.FromSeconds(10));
            Assert.True(isContinue);
            Assert.Equal(0, job.TimesJobDone);
        }

        [Fact]
        public void LockTaskResponsible_WhenLockTakenAndRelease_JobShouldBeDone()
        {
            var job = new Job();

            var @lock = new object();
            Monitor.Enter(@lock);

            var tokenSource = new CancellationTokenSource();
            var lockTaskResponsible =
              new WriteAheadJournal.JournalApplicator.LockTaskResponsible(@lock, tokenSource.Token);

            Task.Run(() => lockTaskResponsible.WaitForTaskToBeDone(job.Do));

            Thread.Sleep(TimeSpan.FromSeconds(1));
            Monitor.Exit(@lock);

            job.Wait(TimeSpan.FromSeconds(10));
            Assert.Equal(1, job.TimesJobDone);
        }

        [Fact]
        public void LockTaskResponsible_WhenLockTaken_ShouldBeDoneByRunTaskIfNotAlreadyRan()
        {
            var job = new Job();

            var @lock = new object();
            Monitor.Enter(@lock);

            var tokenSource = new CancellationTokenSource();
            var lockTaskResponsible =
              new WriteAheadJournal.JournalApplicator.LockTaskResponsible(@lock, tokenSource.Token);

            Task.Run(() => lockTaskResponsible.WaitForTaskToBeDone(job.Do));

            var stop = Stopwatch.StartNew();
            while (job.TimesJobDone == 0 && stop.Elapsed < TimeSpan.FromSeconds(10))
            {
                lockTaskResponsible.RunTaskIfNotAlreadyRan();
                Thread.Sleep(100);
            }

            Assert.Equal(1, job.TimesJobDone);
        }

        [Fact]
        public void LockTaskResponsible_WhenLockTakenAndDoneByRunTaskIfNotAlreadyRan_ShouldBeWaitUntilTheJobDone()
        {
            var job = new Job();

            var @lock = new object();
            Monitor.Enter(@lock);

            var tokenSource = new CancellationTokenSource();
            var lockTaskResponsible =
                new WriteAheadJournal.JournalApplicator.LockTaskResponsible(@lock, tokenSource.Token);

            void LongJob()
            {
                Thread.Sleep(TimeSpan.FromSeconds(1));
                job.Do();
            }

            var isContinueBeforeJobDone = true;
            Task.Run(() =>
            {
                lockTaskResponsible.WaitForTaskToBeDone(LongJob);
                isContinueBeforeJobDone = job.TimesJobDone == 0;
            });

            var stop = Stopwatch.StartNew();
            while (job.TimesJobDone == 0 && stop.Elapsed < TimeSpan.FromSeconds(10))
            {
                lockTaskResponsible.RunTaskIfNotAlreadyRan();
                Thread.Sleep(100);
            }

            Assert.Equal(1, job.TimesJobDone);
            Assert.False(isContinueBeforeJobDone);
        }

        [Fact]
        public void LockTaskResponsible_WhenLockNotTaken_WorkShouldBeDone()
        {
            var job = new Job();

            var @lock = new object();

            var tokenSource = new CancellationTokenSource();
            var lockTaskResponsible =
              new WriteAheadJournal.JournalApplicator.LockTaskResponsible(@lock, tokenSource.Token);

            Task.Run(() => lockTaskResponsible.WaitForTaskToBeDone(job.Do));

            job.Wait(TimeSpan.FromSeconds(10));

            Assert.Equal(1, job.TimesJobDone);
        }

        private class Job
        {
            private readonly AutoResetEvent _autoResetEvent = new AutoResetEvent(false);
            private int _timesJobDone;

            public int TimesJobDone => _timesJobDone;

            public void Do()
            {
                Interlocked.Add(ref _timesJobDone, 1);
                _autoResetEvent.Set();
            }

            public void Wait(TimeSpan time)
            {
                _autoResetEvent.WaitOne(time);
            }
        }
    }
}
