using System;
using System.IO;
using System.Threading;
using FastTests;
using FastTests.Voron;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Issues
{
    public class RavenDB_27226 : StorageTest
    {
        public RavenDB_27226(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.MaxScratchBufferSize = 65536 - 1; // to make ShouldReduceSizeOfCompressionPager() return true
        }

        [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Compression)]
        public void DisposingEnvironmentWhileTheCompressionPagerIsBeingRecreatedMustNotLeakTheNewPager()
        {
            var testingActionWasCalled = false;

            Exception disposeException = null;

            var disposeReachedTheWriteLock = new ManualResetEventSlim(false);
            var disposeReadThePager = new ManualResetEventSlim(false);

            var disposeEnvironmentThread = new Thread(() =>
            {
                try
                {
                    Env.Dispose();
                }
                catch (Exception e)
                {
                    disposeException = e;
                }
            });

            Env.Journal.ForTestingPurposesOnly().OnJournalDispose_BeforeTakingCompressionPagerWriteLock += () =>
                disposeReachedTheWriteLock.Set();

            Env.Journal.ForTestingPurposesOnly().OnJournalDispose_AfterDisposingCompressionPager += () =>
                disposeReadThePager.Set();

            Env.Journal.ForTestingPurposesOnly().OnReduceSizeOfCompressionBufferIfNeeded_RightAfterDisposingCompressionPager += () =>
            {
                testingActionWasCalled = true;

                // dispose the environment in the exact window between disposing the old compression pager and
                // publishing the new one. this is what StorageSpaceMonitor does in production - it calls
                // StorageEnvironment.Cleanup() on a background thread for every index environment, while the
                // index itself can be torn down concurrently
                disposeEnvironmentThread.Start();

                // wait for the disposal to actually arrive at the compression pager instead of guessing at it with a
                // sleep - it first has to get through the flushing lock, WaitForSyncToCompleteOnDispose and the
                // _envDispose drain, which on a busy machine can take longer than any sleep we would pick. if it is
                // not here yet we publish the new pager first, and then even the unfixed teardown disposes it - the
                // test would go green with the bug still in place
                Assert.True(disposeReachedTheWriteLock.Wait(TimeSpan.FromSeconds(30)),
                    "the environment dispose never reached the journal while the compression pager swap was in flight");

                // bounded on purpose: with the fix the disposal is blocked behind the _writeLock we are holding here,
                // so this wait must expire - we then publish, release the lock, and it disposes what it finds.
                // without the fix it takes its read of _compressionPager right here and gets the one disposed above
                disposeReadThePager.Wait(TimeSpan.FromSeconds(2));
            };

            Env.Journal.TryReduceSizeOfCompressionBufferIfNeeded();

            Assert.True(testingActionWasCalled, "the compression pager was not recreated, the test did not exercise anything");

            Assert.True(disposeEnvironmentThread.Join(TimeSpan.FromSeconds(60)), "disposeEnvironmentThread.Join(TimeSpan.FromSeconds(60))");

            Assert.Null(disposeException);

            var compressionPager = Env.Journal.ForTestingPurposesOnly().CompressionPager;

            // if the pager that was published here is not disposed then nothing will ever dispose it - the environment
            // is already gone. its mapping stays alive for the lifetime of the process and on Windows that leaves the
            // file backing it under Temp\ undeletable, which in turn makes the whole index directory impossible to remove
            Assert.True(compressionPager.Disposed,
                $"The compression pager '{compressionPager}' was not disposed together with the storage environment");
        }

        [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Compression)]
        public void DisposeMustNotWaitForeverOnAWriteLockHeldByAStuckSwapAndMustStillNotLeaveTheNewPagerBehind()
        {
            // the teardown runs from StorageEnvironment.Dispose's finally, also after the _envDispose drain gave up on a
            // transaction that is still alive - possibly one stuck inside WriteToJournal, holding _writeLock. waiting for
            // that lock without a bound hangs the dispose and leaves everything after the journal undisposed, so it
            // has to give up and carry on - and the pager published after it gave up must still be disposed by somebody
            var testingActionWasCalled = false;
            var disposeReachedTheWriteLock = false;
            var disposeGaveUpOnTheWriteLock = false;
            var teardownWaitedForThePublish = false;

            var disposeIsAtTheWriteLock = new ManualResetEventSlim(false);
            var disposeGotPastTheWriteLock = new ManualResetEventSlim(false);
            var swapPublished = new ManualResetEventSlim(false);

            Exception disposeException = null;

            var disposeEnvironmentThread = new Thread(() =>
            {
                try
                {
                    Env.Dispose();
                }
                catch (Exception e)
                {
                    disposeException = e;
                }
            });

            // Env.Journal is unreachable once the environment is gone, so keep the reference we mean to interrogate
            var journal = Env.Journal;

            journal.ForTestingPurposesOnly().OnReduceSizeOfCompressionBufferIfNeeded_RightAfterDisposingCompressionPager += () =>
            {
                testingActionWasCalled = true;

                disposeEnvironmentThread.Start();

                disposeReachedTheWriteLock = disposeIsAtTheWriteLock.Wait(TimeSpan.FromSeconds(30));

                // we are holding _writeLock and will not let go of it until the teardown got past it - a stuck writer.
                // bounded so that an unfixed, unbounded teardown fails the test instead of hanging it
                disposeGaveUpOnTheWriteLock = disposeGotPastTheWriteLock.Wait(TimeSpan.FromSeconds(30));
            };

            journal.ForTestingPurposesOnly().OnJournalDispose_BeforeTakingCompressionPagerWriteLock += () => disposeIsAtTheWriteLock.Set();

            journal.ForTestingPurposesOnly().OnJournalDispose_AfterDisposingCompressionPager += () =>
            {
                disposeGotPastTheWriteLock.Set();

                // hold the teardown here until we have published, otherwise the environment finishes tearing itself down
                // and CreateCompressionPager has nothing left to build on: that would be a failed creation, which says
                // nothing about who owns a pager that was created
                teardownWaitedForThePublish = swapPublished.Wait(TimeSpan.FromSeconds(30));
            };

            try
            {
                // this thread is the swap - StorageSpaceMonitor in production
                journal.TryReduceSizeOfCompressionBufferIfNeeded();
            }
            finally
            {
                swapPublished.Set(); // never leave the disposing thread waiting on us
            }

            Assert.True(testingActionWasCalled, "the compression pager was not recreated, the test did not exercise anything");

            Assert.True(disposeReachedTheWriteLock, "the environment was never disposed while the compression pager swap was in flight");

            Assert.True(disposeGaveUpOnTheWriteLock, "the journal dispose waited on _writeLock for as long as its holder kept it, instead of giving up");

            Assert.True(disposeEnvironmentThread.Join(TimeSpan.FromSeconds(60)), "disposeEnvironmentThread.Join(TimeSpan.FromSeconds(60))");

            Assert.True(teardownWaitedForThePublish, "the teardown did not reach the compression pager disposal");

            // giving up on the lock must not turn into an exception: StorageEnvironment.Dispose latches it into an
            // AggregateException and Index._disposeOnce (a DisposeOnce<SingleAttempt>) then rethrows it for the lifetime
            // of the process
            Assert.Null(disposeException);

            var pagerPublishedAfterTheTimeout = journal.ForTestingPurposesOnly().CompressionPager;

            Assert.True(pagerPublishedAfterTheTimeout.Disposed,
                $"The compression pager '{pagerPublishedAfterTheTimeout}' was published after the teardown gave up waiting for the " +
                $"write lock, and nothing disposed it");
        }

        [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Compression)]
        public void CleanupArrivingAfterTheEnvironmentWasDisposedMustNotCreateANewCompressionPager()
        {
            // MaxScratchBufferSize is below the initial compression buffer size (see Configure above), so
            // ShouldReduceSizeOfCompressionPager() would say 'yes' here and without the guard at the top of
            // ReduceSizeOfCompressionBufferIfNeeded this would dispose an already disposed pager and then create,
            // map and publish a brand new one into a journal that nobody owns any more
            var journal = Env.Journal;
            var pagerBeforeTheDispose = journal.ForTestingPurposesOnly().CompressionPager;

            Env.Dispose();

            Assert.True(pagerBeforeTheDispose.Disposed, "the environment dispose did not dispose the compression pager");

            // this is StorageSpaceMonitor / IndexStore.RunIdleOperations arriving late - neither of them synchronises
            // against a teardown, so it happens
            journal.TryReduceSizeOfCompressionBufferIfNeeded();

            Assert.Same(pagerBeforeTheDispose, journal.ForTestingPurposesOnly().CompressionPager);
            Assert.True(journal.ForTestingPurposesOnly().CompressionPager.Disposed);
        }
    }

    // deliberately does NOT lower MaxScratchBufferSize: this test needs ShouldReduceSizeOfCompressionPager() to return
    // false, which is the branch that reaches DiscardWholeFile()
    public class RavenDB_27226_CompressionBufferWithinItsLimit : StorageTest
    {
        public RavenDB_27226_CompressionBufferWithinItsLimit(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Compression)]
        public void CleanupArrivingAfterTheEnvironmentWasDisposedMustNotTouchTheDisposedCompressionPager()
        {
            var journal = Env.Journal;

            Env.Dispose();

            // the compression buffer is within MaxScratchBufferSize, so ReduceSizeOfCompressionBufferIfNeeded takes the
            // 'no need to shrink' branch and calls _compressionPager.DiscardWholeFile(). on a disposed pager that reads
            // AbstractPager.PagerState, whose getter throws ObjectDisposedException - so without the guard at the top of
            // the method this call blows up in the face of whatever background thread happened to make it.
            // note: the 32 bits pagers override DiscardWholeFile() with a no-op, so on those this only asserts the contract
            var error = Record.Exception(() => journal.TryReduceSizeOfCompressionBufferIfNeeded());

            Assert.Null(error);
        }
    }

    public class RavenDB_27226_LeftoverTempFiles : NoDisposalNeeded
    {
        public RavenDB_27226_LeftoverTempFiles(ITestOutputHelper output) : base(output)
        {
        }

        private const string LeakedBufferName = "compression.0000000001.buffers";

        // Windows only: this is where a file can be held in a way that refuses File.Delete and CreateFile. on POSIX the
        // name is unlinked regardless of who has it open, which is exactly why the leak has no visible symptom there
        [RavenMultiplatformFact(RavenTestCategory.Voron, RavenPlatform.Windows)]
        public void DeleteAllTempFilesMustLeaveBehindAFileItCannotDeleteInsteadOfThrowing()
        {
            var path = RavenTestHelper.NewDataPath(nameof(DeleteAllTempFilesMustLeaveBehindAFileItCannotDeleteInsteadOfThrowing), 0);

            try
            {
                string leftover;

                using (var options = (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)StorageEnvironmentOptions.ForPath(path))
                {
                    Directory.CreateDirectory(options.TempPath.FullPath);
                    leftover = Path.Combine(options.TempPath.FullPath, LeakedBufferName);

                    // stands in for a compression buffer whose pager was orphaned by the race: something in this process
                    // still holds it, so it cannot be removed
                    using (File.Open(leftover, FileMode.Create, FileAccess.ReadWrite, FileShare.None))
                    {
                        var error = Record.Exception(() => options.DeleteAllTempFiles());

                        Assert.Null(error);
                        Assert.True(File.Exists(leftover), "the file was expected to be left behind, not deleted");

                        // and the point of leaving it behind: opening an environment over this directory has to keep
                        // working. DirectoryStorageEnvironmentOptions' constructor calls DeleteAllTempFiles(), which is
                        // the UnauthorizedAccessException the customer saw coming out of Index.CreateStorageEnvironmentOptions.
                        // the environment owns the options it is given, so it disposes them for us
                        using (new StorageEnvironment(StorageEnvironmentOptions.ForPath(path)))
                        {
                        }
                    }
                }
            }
            finally
            {
                IOExtensions.DeleteDirectory(path);
            }
        }

        [RavenMultiplatformFact(RavenTestCategory.Voron, RavenPlatform.Windows)]
        public void CreatingATemporaryPagerMustRenameAroundALeftoverItCannotDelete()
        {
            var path = RavenTestHelper.NewDataPath(nameof(CreatingATemporaryPagerMustRenameAroundALeftoverItCannotDelete), 0);

            try
            {
                using (var options = StorageEnvironmentOptions.ForPath(path))
                {
                    Directory.CreateDirectory(options.TempPath.FullPath);

                    var leftover = Path.Combine(options.TempPath.FullPath, LeakedBufferName);

                    using (File.Open(leftover, FileMode.Create, FileAccess.ReadWrite, FileShare.None))
                    {
                        // a fresh environment names its first compression buffer ...0000 and the first shrink names it
                        // ...0001 - so the name DeleteAllTempFiles just left behind is the very next one we ask for
                        using (var pager = options.CreateTemporaryBufferPager(LeakedBufferName, 64 * 1024))
                        {
                            Assert.NotEqual(leftover, pager.FileName.FullPath);
                            Assert.Contains("-ren-", pager.FileName.FullPath);
                        }
                    }
                }
            }
            finally
            {
                IOExtensions.DeleteDirectory(path);
            }
        }

        [RavenMultiplatformFact(RavenTestCategory.Voron, RavenPlatform.Windows)]
        public void CreatingATemporaryPagerMustRenameAroundALeftoverItCannotOpen()
        {
            var path = RavenTestHelper.NewDataPath(nameof(CreatingATemporaryPagerMustRenameAroundALeftoverItCannotOpen), 0);

            try
            {
                using (var options = StorageEnvironmentOptions.ForPath(path))
                {
                    Directory.CreateDirectory(options.TempPath.FullPath);

                    // a name that File.Exists() cannot see and that CreateFile refuses - the same shape as a
                    // delete-pending temp file left behind by a leaked pager, which is what makes GetTemporaryPager fail
                    // at the pager constructor rather than at the File.Delete above it
                    var blocked = Path.Combine(options.TempPath.FullPath, LeakedBufferName);
                    Directory.CreateDirectory(blocked);

                    Assert.False(File.Exists(blocked));

                    using (var pager = options.CreateTemporaryBufferPager(LeakedBufferName, 64 * 1024))
                    {
                        Assert.Contains("-ren-", pager.FileName.FullPath);
                        Assert.True(File.Exists(pager.FileName.FullPath));
                    }
                }
            }
            finally
            {
                IOExtensions.DeleteDirectory(path);
            }
        }

        [RavenMultiplatformFact(RavenTestCategory.Voron, RavenPlatform.Windows)]
        public void DeleteAllTempFilesMustStillDeleteTheFilesItCanDelete()
        {
            var path = RavenTestHelper.NewDataPath(nameof(DeleteAllTempFilesMustStillDeleteTheFilesItCanDelete), 0);

            try
            {
                using (var options = (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)StorageEnvironmentOptions.ForPath(path))
                {
                    Directory.CreateDirectory(options.TempPath.FullPath);

                    var blocked = Path.Combine(options.TempPath.FullPath, LeakedBufferName);
                    var deletable = Path.Combine(options.TempPath.FullPath, "compression.0000000002.buffers");
                    var alsoDeletable = Path.Combine(options.TempPath.FullPath, "scratch.0000000001.buffers");
                    var notATempFile = Path.Combine(options.TempPath.FullPath, "some.file");

                    File.WriteAllText(deletable, string.Empty);
                    File.WriteAllText(alsoDeletable, string.Empty);
                    File.WriteAllText(notATempFile, string.Empty);

                    using (File.Open(blocked, FileMode.Create, FileAccess.ReadWrite, FileShare.None))
                    {
                        options.DeleteAllTempFiles();

                        // one file we cannot remove must not cost us the rest of the sweep - the loop used to be aborted
                        // by the first failure
                        Assert.False(File.Exists(deletable));
                        Assert.False(File.Exists(alsoDeletable));
                        Assert.True(File.Exists(blocked));
                        Assert.True(File.Exists(notATempFile), "DeleteAllTempFiles must only touch .buffers and .tmp files");
                    }
                }
            }
            finally
            {
                IOExtensions.DeleteDirectory(path);
            }
        }
    }
}
