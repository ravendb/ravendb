using System;
using System.Linq;
using Sparrow.Platform;
using Sparrow.Server.Platform;
using Tests.Infrastructure;
using Voron;
using Voron.Global;
using Voron.Impl.Paging;
using Voron.Impl.Scratch;
using Xunit;

namespace FastTests.Voron.ScratchBuffer
{
    public class ScratchMemoryTests : StorageTest
    {
        private const long Mb = Constants.Size.Megabyte;

        // not the global budget: tests that run at the same time would see each other's memory
        private readonly ScratchMemoryBudget _budget = new(limit: 256 * Mb);

        private byte[] _masterKey;

        public ScratchMemoryTests(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
            options.ScratchMemoryBudget = _budget;
            if (_masterKey != null)
                options.Encryption.MasterKey = _masterKey.ToArray();
            base.Configure(options);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void Pages_come_from_memory_while_it_has_room()
        {
            WriteValues("k", count: 100, size: 1000);

            var info = Info();
            Assert.Equal(1, info.MemorySegments);
            Assert.True(MemoryPages(info) > 0);
            Assert.False(ScratchFilesWereUsed(Env));
            Assert.Contains(info.ScratchFilesUsage, f => f.IsMemory);
            Assert.Equal(info.MemorySizeInKB * 1024, _budget.Used);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void Allocations_larger_than_a_segment_go_to_the_scratch_files()
        {
            RequireFileBasedPager();
            Options.MaxScratchBufferSize = 4 * Mb; // 1 MB segments

            WriteValues("k", count: 2, size: 2 * (int)Mb);

            Assert.True(ScratchFilesWereUsed(Env));
            AssertValues("k", count: 2, size: 2 * (int)Mb);

            Env.FlushLogToDataFile();
            RestartDatabase();
            AssertValues("k", count: 2, size: 2 * (int)Mb);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void Memory_grows_in_segments_up_to_the_max_scratch_buffer_size()
        {
            RequireFileBasedPager();
            Options.MaxScratchBufferSize = 4 * Mb; // 4 segments of 1 MB

            WriteValues("k", count: 40, size: 100_000);

            var info = Info();
            Assert.Equal(4, info.MemorySegments);
            Assert.Equal(4096, info.MemorySizeInKB);
            Assert.True(ScratchFilesWereUsed(Env));
            Assert.Equal(4 * Mb, _budget.Used);
            AssertValues("k", count: 40, size: 100_000);

            Env.FlushLogToDataFile();
            RestartDatabase();
            AssertValues("k", count: 40, size: 100_000);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void A_full_budget_sends_pages_to_the_scratch_files()
        {
            _budget.Limit = 2 * Mb;

            WriteValues("k", count: 30, size: 100_000);

            var info = Info();
            Assert.Equal(2048, info.MemorySizeInKB);
            Assert.True(ScratchFilesWereUsed(Env));
            Assert.True(info.GlobalMemoryUnderPressure);
            Assert.Equal(2 * Mb, _budget.Used);
            AssertValues("k", count: 30, size: 100_000);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void Under_pressure_the_larger_environment_gives_back_its_last_segment()
        {
            _budget.Limit = 3 * Mb;
            Options.MaxScratchBufferSize = 4 * Mb; // 1 MB segments

            // this environment takes the whole budget
            WriteValues("k", count: 20, size: 100_000);
            var info = Info();
            Assert.Equal(3, info.MemorySegments);
            Assert.Equal(3 * Mb, _budget.Used);

            using (var otherOptions = StorageEnvironmentOptions.CreateMemoryOnlyForTests())
            {
                otherOptions.ManualFlushing = true;
                otherOptions.ScratchMemoryBudget = _budget;
                otherOptions.MaxScratchBufferSize = 4 * Mb;
                using (var other = new StorageEnvironment(otherOptions))
                {
                    // nothing left for the other one: its pages go to files
                    WriteValues(other, "k", count: 5, size: 1000);
                    var otherInfo = other.ScratchBufferPool.InfoForDebug(other.PossibleOldestReadTransaction(null));
                    Assert.Equal(0, otherInfo.MemorySegments);
                    Assert.True(ScratchFilesWereUsed(other));
                    Assert.True(_budget.UnderPressure);

                    // more than one segment under pressure: the last one takes no more pages and closes once it is not used
                    Env.FlushLogToDataFile();
                    for (int i = 0; i < 4; i++)
                        WriteValues("touch", count: 1, size: 10);

                    info = Info();
                    Assert.Equal(2, info.MemorySegments);
                    Assert.Equal(2 * Mb, _budget.Used);
                    Assert.False(_budget.UnderPressure);
                    AssertValues("k", count: 20, size: 100_000);

                    // and the other one gets memory now
                    WriteValues(other, "j", count: 5, size: 1000);
                    otherInfo = other.ScratchBufferPool.InfoForDebug(other.PossibleOldestReadTransaction(null));
                    Assert.Equal(1, otherInfo.MemorySegments);
                    Assert.True(MemoryPages(otherInfo) > 0);
                }
            }

            Assert.Equal(2 * Mb, _budget.Used);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void Under_pressure_an_idle_environment_gives_back_its_last_segment_on_cleanup()
        {
            _budget.Limit = 3 * Mb;
            Options.MaxScratchBufferSize = 4 * Mb; // 1 MB segments

            WriteValues("k", count: 20, size: 100_000);
            Assert.Equal(3, Info().MemorySegments);

            using (var otherOptions = StorageEnvironmentOptions.CreateMemoryOnlyForTests())
            {
                otherOptions.ManualFlushing = true;
                otherOptions.ScratchMemoryBudget = _budget;
                using (var other = new StorageEnvironment(otherOptions))
                {
                    WriteValues(other, "k", count: 5, size: 1000);
                    Assert.True(_budget.UnderPressure);

                    // no more writes here: the cleanup is what closes the drained segment
                    Env.FlushLogToDataFile();
                    Env.ScratchBufferPool.Cleanup();

                    Assert.Equal(2, Info().MemorySegments);
                    Assert.Equal(2 * Mb, _budget.Used);
                    Assert.False(_budget.UnderPressure);
                    AssertValues("k", count: 20, size: 100_000);
                }
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void Environments_return_their_memory_when_disposed()
        {
            WriteValues("k", count: 10, size: 100_000);
            Assert.True(_budget.Used > 0);

            StopDatabase();
            Assert.Equal(0, _budget.Used);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void Memory_is_reused_after_a_flush()
        {
            Options.MaxScratchBufferSize = 8 * Mb; // 2 MB segments

            // 20 x 500 KB: the flushed pages must be reused
            for (int i = 0; i < 20; i++)
            {
                WriteValues("k", count: 5, size: 100_000, seed: i);
                Env.FlushLogToDataFile();
            }

            Assert.False(ScratchFilesWereUsed(Env));
            Assert.True(Info().MemorySizeInKB <= 8 * 1024);
            AssertValues("k", count: 5, size: 100_000, seed: 19);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void Cleanup_keeps_the_memory()
        {
            WriteValues("k", count: 10, size: 10_000);
            Env.FlushLogToDataFile();

            var before = Info();
            Env.ScratchBufferPool.Cleanup();

            WriteValues("j", count: 10, size: 10_000);

            var after = Info();
            Assert.Equal(before.MemorySegments, after.MemorySegments);
            Assert.True(MemoryPages(after) > 0);
            Assert.False(ScratchFilesWereUsed(Env));
            AssertValues("j", count: 10, size: 10_000);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void A_read_transaction_keeps_reading_its_pages_after_the_memory_grows()
        {
            WriteValues("first", count: 1, size: 50_000);

            using (var read = Env.ReadTransaction())
            {
                var sizeBefore = Info().MemorySizeInKB;

                WriteValues("k", count: 40, size: 100_000);
                Assert.True(Info().MemorySizeInKB > sizeBefore);

                GC.Collect();
                GC.WaitForPendingFinalizers();
                Pager.State.DrainPendingDisposal();

                var tree = read.ReadTree("t");
                Assert.Equal(Value(0, 50_000, 0), tree.Read("first0").Reader.AsSpan().ToArray());
                Assert.Null(tree.Read("k0"));
            }

            AssertValues("k", count: 40, size: 100_000);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void Overflow_shrink_decreases_the_allocated_pages_in_memory()
        {
            using (var tx = Env.WriteTransaction())
            {
                var overflowSize = 4 * Constants.Storage.PageSize;
                var page = tx.LowLevelTransaction.AllocateOverflowRawPage(overflowSize, out _, zeroPage: false);

                var memory = Env.ScratchBufferPool.MemorySegmentsForTesting.Single();
                var allocatedPagesCount = memory.AllocatedPagesCount;

                var reducedOverflowSize = 2 * Constants.Storage.PageSize;
                tx.LowLevelTransaction.ShrinkOverflowPage(page.PageNumber, reducedOverflowSize, tx.LowLevelTransaction.RootObjects);

                var shrinkPages = (overflowSize - reducedOverflowSize) / Constants.Storage.PageSize;
                Assert.Equal(allocatedPagesCount - shrinkPages + 1 /* free space handling allocated one page during shrink */, memory.AllocatedPagesCount);
            }
        }

        [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Encryption)]
        public void Encrypted_environment_keeps_encrypted_pages_in_memory()
        {
            _masterKey = Sodium.GenerateRandomBuffer((int)Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes());
            RequireFileBasedPager();

            WriteValues("k", count: 20, size: 10_000);

            Assert.True(MemoryPages(Info()) > 0);
            Assert.False(ScratchFilesWereUsed(Env));
            AssertValues("k", count: 20, size: 10_000);

            Env.FlushLogToDataFile();
            AssertValues("k", count: 20, size: 10_000);

            RestartDatabase();
            AssertValues("k", count: 20, size: 10_000);
        }

        [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Encryption)]
        public void Encrypted_environment_keeps_its_pages_when_the_memory_grows()
        {
            _masterKey = Sodium.GenerateRandomBuffer((int)Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes());
            RequireFileBasedPager();

            for (int i = 0; i < 4; i++)
            {
                WriteValues("k" + i, count: 10, size: 100_000, seed: i);
                AssertValues("k" + i, count: 10, size: 100_000, seed: i);
            }

            Assert.True(Info().MemorySizeInKB > 1024);
            Assert.False(ScratchFilesWereUsed(Env));

            Env.FlushLogToDataFile();
            for (int i = 0; i < 4; i++)
                AssertValues("k" + i, count: 10, size: 100_000, seed: i);

            RestartDatabase();
            for (int i = 0; i < 4; i++)
                AssertValues("k" + i, count: 10, size: 100_000, seed: i);
        }

        [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Encryption)]
        public void Encrypted_environment_can_fill_a_segment_up_to_its_last_page()
        {
            _masterKey = Sodium.GenerateRandomBuffer((int)Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes());
            RequireFileBasedPager();
            Options.MaxScratchBufferSize = 4 * Mb; // 1 MB segments

            // single pages fill the segments up to their last page, and the commit encrypts that page back into it
            using (var tx = Env.WriteTransaction())
            {
                for (int i = 0; i < 300; i++)
                    tx.CreateTree("t" + i).Add("k", Value(i, 100, 0));
                tx.Commit();
            }

            var info = Info();
            Assert.True(info.MemorySegments > 1);
            Assert.False(Env.Options.IsCatastrophicFailureSet);

            using (var tx = Env.ReadTransaction())
            {
                for (int i = 0; i < 300; i++)
                    Assert.Equal(Value(i, 100, 0), tx.ReadTree("t" + i).Read("k").Reader.AsSpan().ToArray());
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public unsafe void Anonymous_pager_maps_two_views_of_the_same_memory_and_grows_up_to_its_max()
        {
            var (pager, state) = Pager.CreateAnonymous(Options, "anonymous-pager-test", Mb, 3 * Mb, Pal.OpenFileFlags.None);
            using (pager)
            {
                Assert.True(pager.IsAnonymous);
                Assert.Equal(Mb, state.TotalAllocatedSize);

                state.WriteAddress[100] = 42;
                Assert.Equal(42, state.ReadAddress[100]);

                var old = state;
                pager.EnsureContinuous(ref state, 0, (int)(2 * Mb / Constants.Storage.PageSize) + 1);
                var grownSize = 2 * Mb + Pager.AnonymousGranularity;
                Assert.Equal(grownSize, state.TotalAllocatedSize);
                Assert.Equal(42, state.ReadAddress[100]);

                // the grown range is committed and seen by both views, and the old views still share the memory
                state.WriteAddress[grownSize - 1] = 7;
                Assert.Equal(7, state.ReadAddress[grownSize - 1]);
                old.WriteAddress[200] = 9;
                Assert.Equal(9, state.ReadAddress[200]);

                Assert.ThrowsAny<Exception>(() => pager.EnsureContinuous(ref state, 0, (int)(4 * Mb / Constants.Storage.PageSize)));
                Assert.Equal(grownSize, state.TotalAllocatedSize);
            }
        }

        private ScratchBufferPoolInfo Info() => Env.ScratchBufferPool.InfoForDebug(Env.PossibleOldestReadTransaction(null));

        private static long MemoryPages(ScratchBufferPoolInfo info) => info.ScratchFilesUsage.Where(f => f.IsMemory).Sum(f => f.AllocatedPagesCount);

        // a scratch file took pages if it moved from its start, was reset, or more than one exists
        private static bool ScratchFilesWereUsed(StorageEnvironment env)
        {
            var file = env.ScratchBufferPool._current.File;
            var files = env.ScratchBufferPool.InfoForDebug(env.PossibleOldestReadTransaction(null)).ScratchFilesUsage.Count(f => f.IsMemory == false);
            return file.LastUsedPage > 0 || file.DebugInfo.NumberOfResets > 0 || files > 1;
        }

        private void WriteValues(string prefix, int count, int size, int seed = 0) => WriteValues(Env, prefix, count, size, seed);

        private static void WriteValues(StorageEnvironment env, string prefix, int count, int size, int seed = 0)
        {
            using (var tx = env.WriteTransaction())
            {
                var tree = tx.CreateTree("t");
                for (int i = 0; i < count; i++)
                    tree.Add(prefix + i, Value(i, size, seed));
                tx.Commit();
            }
        }

        private void AssertValues(string prefix, int count, int size, int seed = 0)
        {
            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("t");
                for (int i = 0; i < count; i++)
                    Assert.Equal(Value(i, size, seed), tree.Read(prefix + i).Reader.AsSpan().ToArray());
            }
        }

        private static byte[] Value(int i, int size, int seed)
        {
            var value = new byte[size];
            new Random(i * 397 + seed).NextBytes(value);
            return value;
        }
    }
}
