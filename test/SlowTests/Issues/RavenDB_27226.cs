using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_27226 : RavenTestBase
    {
        public RavenDB_27226(ITestOutputHelper output) : base(output)
        {
        }

        private const string IndexName = "Plans/ByMetadata";

        private static readonly string ReplacementIndexName = Constants.Documents.Indexing.SideBySideIndexNamePrefix + IndexName;

        [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Voron)]
        public async Task ReplacingAnIndexWhileItsCompressionPagerIsRecreatedMustNotLeakTheNewPager()
        {
            using (var store = GetDocumentStore(new Options
                   {
                       // the environment has to be on disk: ReplaceIndexes only restarts it (and therefore only
                       // disposes it) when the index is not running in memory
                       RunInMemory = false,
                       Path = NewDataPath(),
                       ModifyDatabaseRecord = record =>
                       {
                           // the compression buffer has to outgrow this, otherwise ShouldReduceSizeOfCompressionPager
                           // never returns true and there is no pager swap to race against
                           record.Settings["Storage.MaxScratchBufferSizeInMb"] = "1";
                       }
                   }))
            {
                await InsertPlans(store, count: 3_000);

                await PutIndex(store, "from p in docs.Plans select new { p.Name, p.Description }");
                await Indexes.WaitForIndexingAsync(store);

                var database = await GetDatabase(store.Database);

                // the same index, mapping one more field - this is what makes the server build a side by side
                // replacement and, once it is done, swap it in through IndexStore.ReplaceIndexes
                await PutIndex(store, "from p in docs.Plans select new { p.Name, p.Description, p.Owner }");

                var replacement = WaitForReplacementIndex(database);
                var replacementEnvironment = replacement._environment;
                var replacementDirectory = replacementEnvironment.Options.BasePath.FullPath;
                var journal = replacementEnvironment.Journal;

                var armed = false;
                var compressionPagerWasRecreated = false;
                var raceWasExercised = false;
                var swapperIsParked = new ManualResetEventSlim(false);
                var disposeReachedTheWriteLock = new ManualResetEventSlim(false);
                var swapperPublished = new ManualResetEventSlim(false);
                Exception swapperError = null;
                Thread swapper = null;

                var events = new System.Collections.Concurrent.ConcurrentQueue<string>();

                database.IndexStore.ForTestingPurposesOnly().DuringIndexReplacement_AfterUpdatingCollectionOfIndexes += () =>
                    events.Enqueue($"after-updating-collection [{string.Join("|", database.IndexStore.GetIndexes().Select(i => i.Name))}]");

                journal.ForTestingPurposesOnly().OnReduceSizeOfCompressionBufferIfNeeded_RightAfterDisposingCompressionPager += () =>
                {
                    if (Volatile.Read(ref armed) == false)
                    {
                        // a trim that happened on its own while the replacement was building - not our race.
                        // note that ReplaceIndexes is preceded by FlushAndSync -> StorageEnvironment.Cleanup(),
                        // which trims the compression buffer back down to MaxScratchBufferSize
                        events.Enqueue("reduce-hook (not armed)");
                        return;
                    }

                    compressionPagerWasRecreated = true;
                    events.Enqueue($"reduce-hook armed, dispose already past the lock: {disposeReachedTheWriteLock.IsSet}");
                    swapperIsParked.Set();

                    // stay inside the swap, holding the write lock, until the environment dispose is at the door of
                    // that very same lock. bounded, so if the window is somehow missed this degrades to a slow test
                    raceWasExercised = disposeReachedTheWriteLock.Wait(TimeSpan.FromSeconds(30));

                    // returning now publishes the new pager. without the fix the disposing thread has already taken
                    // its read of _compressionPager and got the one we just disposed, so this one is born orphaned
                };

                journal.ForTestingPurposesOnly().OnJournalDispose_BeforeTakingCompressionPagerWriteLock += () =>
                {
                    events.Enqueue("journal-dispose reached the write lock");
                    disposeReachedTheWriteLock.Set();
                };

                journal.ForTestingPurposesOnly().OnJournalDispose_AfterDisposingCompressionPager += () =>
                {
                    // hold the disposal here until the swap has published its pager. otherwise the environment
                    // finishes disposing and ReplaceIndexes moves the index directory away before the swap gets to
                    // create anything, and then there is no orphaned pager to catch - just a failed creation
                    events.Enqueue("journal-dispose disposed a pager, waiting for the swap to publish");
                    swapperPublished.Wait(TimeSpan.FromSeconds(30));
                };

                database.IndexStore.ForTestingPurposesOnly().DuringIndexReplacement_OnOldIndexDeletion += () =>
                {
                    if (Volatile.Read(ref armed))
                        return; // ReplaceIndexes retries this block on failure, one swapper is enough

                    events.Enqueue("on-old-index-deletion");
                    Volatile.Write(ref armed, true);

                    // recreate the compression pager while the index is still alive, so that the swap is in flight
                    // when ReplaceIndexes gets to RestartEnvironment and disposes the environment underneath it.
                    // in production this is StorageSpaceMonitor calling StorageEnvironment.Cleanup() on a background
                    // thread for every index environment, with nothing synchronising it against the teardown
                    swapper = new Thread(() =>
                    {
                        try
                        {
                            journal.TryReduceSizeOfCompressionBufferIfNeeded();
                            events.Enqueue($"swap finished, pager is now {journal.ForTestingPurposesOnly().CompressionPager}");
                        }
                        catch (Exception e)
                        {
                            swapperError = e;
                        }
                        finally
                        {
                            swapperPublished.Set(); // never leave the disposing thread waiting on us
                        }
                    });

                    swapper.Start();

                    swapperIsParked.Wait(TimeSpan.FromSeconds(30));
                };

                // note that the replacement disappears from _indexes at the very start of ReplaceIndexes
                // (_indexes.ReplaceIndex), so its absence says nothing about the swap being over - the rename, the old
                // index deletion and RestartEnvironment all still lie ahead. wait for our own hooks instead
                Assert.True(WaitForValue(() => Volatile.Read(ref armed), true, timeout: 120_000),
                    $"the swap never reached DuringIndexReplacement_OnOldIndexDeletion. events: {Log(events)}");
                Assert.NotNull(swapper);
                Assert.True(swapper.Join(TimeSpan.FromSeconds(30)), "the recreating thread did not finish");
                // without the fix the swap and the disposal are not mutually exclusive, so the swap can also blow up
                // on a half torn down environment instead of getting far enough to leak its pager
                Assert.True(swapperError == null,
                    $"recreating the compression pager ran into the environment being disposed underneath it. " +
                    $"events: {Log(events)}{Environment.NewLine}{swapperError}");

                Assert.True(compressionPagerWasRecreated,
                    "the compression pager was never recreated during the replacement, so the test did not exercise anything. " +
                    "the compression buffer probably stayed below Storage.MaxScratchBufferSizeInMb");

                Assert.True(raceWasExercised,
                    "the environment was disposed either before or long after the compression pager swap, so the two never " +
                    "overlapped and the test proves nothing");

                // the environment ReplaceIndexes restarted is gone, so whatever its journal is still pointing at is
                // unreachable. if that pager was not disposed its mapping lives until the process exits, and because
                // temp buffer pagers are created with deleteOnClose the file under Temp\ is only removed when the
                // pager is - which is why the customer was left with a compression.*.buffers that nothing could delete
                var pagerPublishedDuringTheRace = journal.ForTestingPurposesOnly().CompressionPager;

                // the symptom the customer actually reported. a leaked pager keeps its file mapped, and because temp
                // buffer pagers are created with deleteOnClose the file is also left in delete-pending state, so every
                // later attempt to remove it is refused - which is what makes the whole index directory undeletable.
                // when the swap and the disposal are mutually exclusive there is no leak, the directory is moved away
                // by ReplaceIndexes and there is nothing left here to delete
                if (Directory.Exists(replacementDirectory))
                {
                    var ioFailure = Record.Exception(() => IOExtensions.DeleteDirectory(replacementDirectory));

                    Assert.True(ioFailure == null,
                        $"'{replacementDirectory}' could not be removed - Raven.Server is still holding a file inside it. " +
                        $"this is the failure the customer reported:{Environment.NewLine}{ioFailure}");
                }

                // ReplaceIndexes is still running on the indexing thread, so give the disposal a chance to finish.
                // with the bug this never becomes true - the disposing thread already took the pager the swap had
                // just discarded, and the one it published instead is owned by nobody
                Assert.True(WaitForValue(() => pagerPublishedDuringTheRace.Disposed, true, timeout: 30_000),
                    $"The compression pager '{pagerPublishedDuringTheRace}' of the replaced index was not disposed with its " +
                    $"storage environment. events: {Log(events)}");

                // and the index itself has to be alive and holding the new definition
                Assert.True(WaitForValue(() => database.IndexStore.GetIndex(IndexName)?.Definition.MapFields.ContainsKey("Owner") == true,
                    true, timeout: 30_000), "the replaced index is not there with its new definition");
            }
        }

        [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Voron)]
        public async Task DeletingAnIndexWhileItsCompressionPagerIsBeingRecreatedMustNotLeaveItsDirectoryUndeletable()
        {
            using (var store = GetDocumentStore(new Options
                   {
                       // what leaks is a memory mapping over a file under the index' Temp\, so the environment
                       // has to be on disk
                       RunInMemory = false,
                       Path = NewDataPath(),
                       ModifyDatabaseRecord = record =>
                       {
                           // ShouldReduceSizeOfCompressionPager only returns true once the compression buffer has
                           // outgrown this. without that there is no pager swap to race against
                           record.Settings["Storage.MaxScratchBufferSizeInMb"] = "1";
                       }
                   }))
            {
                await InsertPlans(store, count: 3_000);

                await PutIndex(store, "from p in docs.Plans select new { p.Name, p.Description }");
                await Indexes.WaitForIndexingAsync(store);

                // the swap below parks while holding the journal's _writeLock, and a write transaction on this index
                // would block on that same lock. Index.Dispose joins the indexing thread with no timeout at all
                // (Index.cs:384), so an indexing batch that starts after WaitForIndexing would hang the teardown
                await store.Maintenance.SendAsync(new StopIndexingOperation());

                var database = await GetDatabase(store.Database);

                var index = database.IndexStore.GetIndex(IndexName);
                Assert.NotNull(index);

                var environment = index._environment;
                var journal = environment.Journal;
                var indexDirectory = environment.Options.BasePath.FullPath;
                var tempDirectory = environment.Options.TempPath.FullPath;

                var swapIsInTheWindow = new ManualResetEventSlim(false);
                var journalDisposeIsAboutToReadThePager = new ManualResetEventSlim(false);
                var journalDisposeReadThePager = new ManualResetEventSlim(false);

                journal.ForTestingPurposesOnly().OnReduceSizeOfCompressionBufferIfNeeded_RightAfterDisposingCompressionPager += () =>
                {
                    // the old compression pager is already disposed and the new one is not published yet. in
                    // production this is a background StorageEnvironment.Cleanup() - StorageSpaceMonitor, or
                    // IndexStore.RunIdleOperations -> Index.Cleanup - sitting in this exact window while something
                    // else tears the same index down. nothing synchronises the two
                    swapIsInTheWindow.Set();

                    if (journalDisposeIsAboutToReadThePager.Wait(TimeSpan.FromSeconds(60)) == false)
                        throw new TimeoutException("the index was never disposed while the compression pager swap was in flight");

                    // wait for the teardown to actually take its read of _compressionPager rather than guessing at it
                    // with a sleep - otherwise the teardown can occasionally overtake us, dispose the pager we
                    // publish below and hide the leak.
                    // bounded on purpose: once the swap and the teardown are made mutually exclusive, the teardown is
                    // blocked behind us here and this wait must expire instead of deadlocking. it then publishes,
                    // releases the lock, and the teardown disposes the pager it finds - which is the fixed behaviour
                    journalDisposeReadThePager.Wait(TimeSpan.FromSeconds(2));
                };

                journal.ForTestingPurposesOnly().OnJournalDispose_BeforeTakingCompressionPagerWriteLock += () =>
                    journalDisposeIsAboutToReadThePager.Set();

                journal.ForTestingPurposesOnly().OnJournalDispose_AfterDisposingCompressionPager += () =>
                    journalDisposeReadThePager.Set();

                Exception swapError = null;

                var swapper = new Thread(() =>
                {
                    try
                    {
                        journal.TryReduceSizeOfCompressionBufferIfNeeded();
                    }
                    catch (Exception e)
                    {
                        swapError = e;
                    }
                })
                {
                    Name = "compression pager swap",
                    IsBackground = true
                };

                swapper.Start();

                Assert.True(swapIsInTheWindow.Wait(TimeSpan.FromSeconds(30)),
                    $"the compression pager of '{IndexName}' was never recreated, so the test did not exercise anything. " +
                    "it probably never outgrew Storage.MaxScratchBufferSizeInMb");

                // the first half of DeleteIndexInternal (IndexStore.cs:1191) - disposing the index disposes the
                // storage environment, and with it the journal, which reads _compressionPager while the swap is
                // parked in the middle of replacing it.
                // note that we must not call DeleteIndexInternal itself here: its second half removes the whole
                // index directory, and the parked swap would then have nowhere to create its file and would simply
                // fail. in production those two halves are separated by everything the environment teardown still
                // has to do after the journal, which is exactly the window the swap published into
                index.Dispose();

                Assert.True(swapper.Join(TimeSpan.FromSeconds(60)), "the thread recreating the compression pager did not finish");
                Assert.Null(swapError);

                var leftovers = Directory.Exists(tempDirectory) ? Directory.GetFiles(tempDirectory) : Array.Empty<string>();

                // the customer's failure. File.Delete on the leaked buffer is the innermost frame of both traces they
                // collected - DeleteAllTempFiles when the index was recreated (StorageEnvironmentOptions.cs:788) and
                // IOExtensions.TryHandlingError when it was deleted (IOExtensions.cs:176) - and it threw
                // UnauthorizedAccessException: Access to the path '...\Temp\compression.<n>.buffers' is denied.
                // it does NOT throw on every Windows, so do not rely on it. measured on Windows 11 26200 over the
                // full flag matrix: an open mapped section makes no difference at all, and with FILE_SHARE_DELETE -
                // which WindowsMemoryMapPager.cs:79 always passes - the name is unlinked immediately. the customer
                // hit ERROR_ACCESS_DENIED for the same shape of file on Windows Server 2019 (17763), also NTFS, with
                // Restart Manager naming Raven.Server as the holder - i.e. a leaked deleteOnClose buffer left
                // delete-pending. the behaviour changed somewhere between those two builds.
                // so here this and the DeleteDirectory below both pass and only the leak assert at the end fires -
                // which is why that assert, and not these, is the contract of this test
                foreach (var leaked in leftovers)
                {
                    File.Delete(leaked);

                    // the file is created with FILE_FLAG_DELETE_ON_CLOSE, so its delete disposition was already set
                    // before the leaked pager mapped it. deleting it again can therefore report success without
                    // removing the name - the name goes only when the last handle closes, which never happens here
                    Assert.False(File.Exists(leaked),
                        $"'{leaked}' was reported as deleted but is still on disk - the leaked pager still holds it open");
                }

                // and the wrapped form, exactly as IndexStore.DeleteIndexInternal raises it (IndexStore.cs:1236):
                // IOException: Failed to get Write access a file at <...\Temp\compression.<n>.buffers>
                IOExtensions.DeleteDirectory(indexDirectory);

                // the defect itself, in case this OS lets both deletes through: the storage environment is gone, so a
                // pager published into its journal after the teardown read _compressionPager is owned by nobody and
                // its mapping lives until the process exits. this check does not depend on any delete semantics
                var pagerPublishedDuringTheRace = journal.ForTestingPurposesOnly().CompressionPager;

                Assert.True(pagerPublishedDuringTheRace.Disposed,
                    $"the compression pager '{pagerPublishedDuringTheRace}' was published into a journal that was already " +
                    $"disposed, so nothing will ever dispose it. files left behind in '{tempDirectory}': " +
                    $"{(leftovers.Length == 0 ? "none" : string.Join(", ", leftovers))}");
            }
        }

        private const string LeakedBufferName = "compression.0000000001.buffers";

        // the customer's other trace, and the one that made the database unusable rather than merely untidy:
        // DeleteAllTempFiles (StorageEnvironmentOptions.cs:788) threw UnauthorizedAccessException on
        // '...\Temp\compression.0000000001.buffers' out of Index.CreateStorageEnvironmentOptions -> Index.Initialize ->
        // MapIndex.CreateNew -> IndexStore.CreateIndexFromDefinition -> HandleStaticIndexChange. that exception is
        // caught by HandleChangesForStaticIndexes (IndexStore.cs:450), which puts a FaultyInMemoryIndex in place of the
        // real index - so the index appears to come back and can never serve a query, for as long as the process lives.
        // the tests above cover how the file comes to be leaked; this one covers having to live with one
        //
        // Windows only: this is where a file can be held in a way that refuses File.Delete. on POSIX the name is
        // unlinked regardless of who holds it open, which is exactly why the leak has no visible symptom there
        [RavenMultiplatformFact(RavenTestCategory.Indexes | RavenTestCategory.Voron, RavenPlatform.Windows)]
        public async Task AnIndexMustBeCreatedOverALeakedTemporaryFileThatCannotBeDeleted()
        {
            const int planCount = 10;

            using (var store = GetDocumentStore(new Options
                   {
                       // the leftover is a file under the index' Temp\, so the environment has to be on disk
                       RunInMemory = false,
                       Path = NewDataPath()
                   }))
            {
                await InsertPlans(store, count: planCount);

                await PutIndex(store, "from p in docs.Plans select new { p.Name, p.Description }");
                await Indexes.WaitForIndexingAsync(store);

                var database = await GetDatabase(store.Database);

                var index = database.IndexStore.GetIndex(IndexName);
                Assert.NotNull(index);

                // ask the running index where its files live instead of reconstructing the layout here - this also
                // covers a configured Indexing.TempPath, where Temp\ is not under the index directory at all
                var indexDirectory = index._environment.Options.BasePath.FullPath;
                var tempDirectory = index._environment.Options.TempPath.FullPath;

                // delete the index cleanly first: the leftover has to be the only thing the recreated environment finds,
                // and planting it before the deletion would just race that deletion for it
                await store.Maintenance.SendAsync(new DeleteIndexOperation(IndexName));

                Assert.True(WaitForValue(() => database.IndexStore.GetIndex(IndexName) == null && Directory.Exists(indexDirectory) == false,
                    true, timeout: 30_000), $"'{IndexName}' and its directory '{indexDirectory}' were not removed");

                Directory.CreateDirectory(tempDirectory);

                var leftover = Path.Combine(tempDirectory, LeakedBufferName);

                // stands in for a compression buffer whose pager was orphaned by the race the tests above reproduce:
                // something in this process still holds it, so File.Delete on it is refused. the real leak is a mapping
                // and FileShare.None is the closest we can get to that on demand - DeleteAllTempFiles sees the same
                // refusal either way, which is all this test is about
                using (File.Open(leftover, FileMode.Create, FileAccess.ReadWrite, FileShare.None))
                {
                    await PutIndex(store, "from p in docs.Plans select new { p.Name, p.Description }");

                    Raven.Server.Documents.Indexes.Index recreated = null;

                    Assert.True(WaitForValue(() =>
                    {
                        recreated = database.IndexStore.GetIndex(IndexName);
                        return recreated != null;
                    }, true, timeout: 30_000), $"'{IndexName}' was never created over the leftover '{leftover}'");

                    // the whole point: opening the environment must not be aborted by a temp file we cannot remove
                    var faulty = recreated as Raven.Server.Documents.Indexes.Errors.FaultyInMemoryIndex;

                    Assert.True(faulty == null, faulty == null
                        ? null
                        : $"'{IndexName}' came up faulty - initializing it over the leftover '{leftover}' threw. " +
                          $"this is the failure the customer reported:{Environment.NewLine}" +
                          $"{string.Join(Environment.NewLine, faulty.GetErrors().Select(e => e.Error))}");

                    // and we really did exercise the tolerance instead of quietly deleting the file and moving on
                    Assert.True(File.Exists(leftover),
                        $"'{leftover}' is gone, so the environment never had to tolerate anything and this test proves nothing");

                    // finally, the index has to be usable rather than merely constructed
                    await Indexes.WaitForIndexingAsync(store);

                    var stats = await store.Maintenance.SendAsync(new GetIndexStatisticsOperation(IndexName));

                    Assert.Equal(planCount, (int)stats.EntriesCount);
                }
            }
        }

        #region OnlyTemporaryFilesAreLeftBehind

        // this predicate is what decides whether DeleteIndexInternal is allowed to report an index as deleted after
        // IOExtensions.DeleteDirectory failed on it. saying 'true' while actual index storage survived would let the very
        // next HandleStaticIndexChange open a second environment on top of a live one, so every case here matters

        [RavenFact(RavenTestCategory.Indexes)]
        public void OnlyTemporaryFilesAreLeftBehind_ADirectoryThatIsGoneCountsAsDeleted()
        {
            using (var dir = new ScratchDirectory())
            {
                Directory.Delete(dir.IndexPath, recursive: true);

                Assert.True(Raven.Server.Documents.Indexes.IndexStore.OnlyTemporaryFilesAreLeftBehind(dir.IndexPath, dir.TempPath, logger: null));
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void OnlyTemporaryFilesAreLeftBehind_LeftoverTemporaryFilesAreAccepted()
        {
            using (var dir = new ScratchDirectory())
            {
                dir.WriteTempFile("compression.0000000001.buffers");
                dir.WriteTempFile("scratch.0000000003.buffers");
                dir.WriteTempFile("lucene-cache-1234.tmp");

                Assert.True(Raven.Server.Documents.Indexes.IndexStore.OnlyTemporaryFilesAreLeftBehind(dir.IndexPath, dir.TempPath, logger: null));
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void OnlyTemporaryFilesAreLeftBehind_AnEmptyDirectoryIsRejected()
        {
            using (var dir = new ScratchDirectory())
            {
                // the directory could not be removed and there is not a single temporary file to explain why. we have no
                // idea what is holding it, so reporting the index as deleted would be a guess
                Assert.False(Raven.Server.Documents.Indexes.IndexStore.OnlyTemporaryFilesAreLeftBehind(dir.IndexPath, dir.TempPath, logger: null));
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void OnlyTemporaryFilesAreLeftBehind_AnEmptyTempDirectoryIsRejected()
        {
            using (var dir = new ScratchDirectory())
            {
                Directory.CreateDirectory(dir.TempPath);

                Assert.False(Raven.Server.Documents.Indexes.IndexStore.OnlyTemporaryFilesAreLeftBehind(dir.IndexPath, dir.TempPath, logger: null));
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void OnlyTemporaryFilesAreLeftBehind_SurvivingIndexStorageIsRejected()
        {
            using (var dir = new ScratchDirectory())
            {
                dir.WriteTempFile("compression.0000000001.buffers");
                dir.WriteIndexFile("Raven.voron");

                Assert.False(Raven.Server.Documents.Indexes.IndexStore.OnlyTemporaryFilesAreLeftBehind(dir.IndexPath, dir.TempPath, logger: null));
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void OnlyTemporaryFilesAreLeftBehind_ASurvivingJournalIsRejected()
        {
            using (var dir = new ScratchDirectory())
            {
                dir.WriteIndexFile(Path.Combine("Journals", "0000000000000000000.journal"));

                Assert.False(Raven.Server.Documents.Indexes.IndexStore.OnlyTemporaryFilesAreLeftBehind(dir.IndexPath, dir.TempPath, logger: null));
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void OnlyTemporaryFilesAreLeftBehind_AFileUnderTempThatIsNotATempFileIsRejected()
        {
            using (var dir = new ScratchDirectory())
            {
                // gated exactly like DeleteAllTempFiles - anything the environment would not remove on its own is not
                // something we get to assume is harmless
                dir.WriteTempFile("Raven.voron");

                Assert.False(Raven.Server.Documents.Indexes.IndexStore.OnlyTemporaryFilesAreLeftBehind(dir.IndexPath, dir.TempPath, logger: null));
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void OnlyTemporaryFilesAreLeftBehind_ATempLookingFileOutsideTempIsRejected()
        {
            using (var dir = new ScratchDirectory())
            {
                dir.WriteIndexFile("compression.0000000001.buffers");

                Assert.False(Raven.Server.Documents.Indexes.IndexStore.OnlyTemporaryFilesAreLeftBehind(dir.IndexPath, dir.TempPath, logger: null));
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void OnlyTemporaryFilesAreLeftBehind_AnUnknownTempPathIsRejected()
        {
            using (var dir = new ScratchDirectory())
            {
                dir.WriteTempFile("compression.0000000001.buffers");

                // Index._environment is null for a faulty in-memory index, and after a failed initialization. we cannot
                // tell what is temporary without it
                Assert.False(Raven.Server.Documents.Indexes.IndexStore.OnlyTemporaryFilesAreLeftBehind(dir.IndexPath, tempPath: null, logger: null));
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void OnlyTemporaryFilesAreLeftBehind_ATempPathOutsideTheIndexDirectoryDoesNotWhitelistAnything()
        {
            using (var dir = new ScratchDirectory())
            {
                // Indexing.TempPath can point at another drive entirely. then nothing under the index directory is
                // temporary, and a leftover there has to be rejected
                dir.WriteIndexFile("compression.0000000001.buffers");

                Assert.False(Raven.Server.Documents.Indexes.IndexStore.OnlyTemporaryFilesAreLeftBehind(
                    dir.IndexPath, Path.Combine(dir.Root, "SomewhereElse"), logger: null));
            }
        }

        private sealed class ScratchDirectory : IDisposable
        {
            public string Root { get; }

            public string IndexPath { get; }

            public string TempPath { get; }

            public ScratchDirectory()
            {
                Root = RavenTestHelper.NewDataPath(nameof(ScratchDirectory), 0, forceCreateDir: true);
                IndexPath = Path.Combine(Root, "Indexes", "Plans_ByMetadata");
                TempPath = Path.Combine(IndexPath, "Temp");

                Directory.CreateDirectory(IndexPath);
            }

            public void WriteIndexFile(string relativePath) => Write(Path.Combine(IndexPath, relativePath));

            public void WriteTempFile(string name) => Write(Path.Combine(TempPath, name));

            private static void Write(string path)
            {
                Directory.CreateDirectory(Path.GetDirectoryName(path));
                File.WriteAllText(path, string.Empty);
            }

            public void Dispose() => IOExtensions.DeleteDirectory(Root);
        }

        #endregion

        private static async Task PutIndex(Raven.Client.Documents.IDocumentStore store, string map)
        {
            await store.Maintenance.SendAsync(new PutIndexesOperation(new IndexDefinition
            {
                Name = IndexName,
                Type = IndexType.Map,
                Maps = { map },
                Fields = new Dictionary<string, IndexFieldOptions>
                {
                    // analysed, stored and with term vectors so that indexing this much text produces transactions
                    // large enough to grow the compression buffer past Storage.MaxScratchBufferSizeInMb
                    ["Description"] = new IndexFieldOptions
                    {
                        Indexing = FieldIndexing.Search,
                        Storage = FieldStorage.Yes,
                        TermVector = FieldTermVector.WithPositionsAndOffsets
                    }
                }
            }));
        }

        private static string Log(System.Collections.Concurrent.ConcurrentQueue<string> events) => string.Join(" -> ", events);

        private static Raven.Server.Documents.Indexes.Index WaitForReplacementIndex(Raven.Server.Documents.DocumentDatabase database)
        {
            Raven.Server.Documents.Indexes.Index replacement = null;

            Assert.True(WaitForValue(() =>
            {
                replacement = database.IndexStore.GetIndex(ReplacementIndexName);
                return replacement != null;
            }, true, timeout: 30_000), $"'{ReplacementIndexName}' was never created");

            return replacement;
        }

        private static async Task InsertPlans(Raven.Client.Documents.IDocumentStore store, int count)
        {
            var description = new string('x', 32);
            var words = new List<string>(64);
            for (var i = 0; i < 64; i++)
                words.Add($"{description}{i}");

            var text = string.Join(' ', words); // ~2 KB of distinct-ish terms per document

            await using (var bulk = store.BulkInsert())
            {
                for (var i = 0; i < count; i++)
                {
                    await bulk.StoreAsync(new Plan
                    {
                        Name = $"plan-{i:D6}",
                        Owner = $"owner-{i % 97}",
                        Description = $"{i} {text}"
                    });
                }
            }
        }

        private sealed class Plan
        {
            public string Id { get; set; }

            public string Name { get; set; }

            public string Owner { get; set; }

            public string Description { get; set; }
        }
    }
}
