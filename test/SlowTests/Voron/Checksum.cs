using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using FastTests.Utils;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Voron.Impl.Paging;
using Voron.Global;

namespace SlowTests.Voron
{
    public class Checksum : StorageTest
    {
        public Checksum(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Voron)]
        public unsafe void ValidatePageChecksumShouldDetectDataCorruption()
        {
            // Create some random data
            var treeNames = new List<string>();

            var random = new Random();

            var value1 = new byte[random.Next(1024 * 1024 * 2)];
            var value2 = new byte[random.Next(1024 * 1024 * 2)];

            random.NextBytes(value1);
            random.NextBytes(value2);

            const int treeCount = 5;
            const int recordCount = 6;

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPathForTests(DataDir)))
            {
                env.Options.ManualFlushing = true;

                for (int i = 0; i < treeCount; i++)
                {
                    using (var tx = env.WriteTransaction())
                    {
                        string name = "tree/" + i;
                        treeNames.Add(name);

                        var tree = tx.CreateTree(name);

                        for (int j = 0; j < recordCount; j++)
                        {
                            tree.Add(string.Format("{0}/items/{1}", name, j), j % 2 == 0 ? value1 : value2);
                        }

                        tx.Commit();
                    }
                }
                env.FlushLogToDataFile();
            }

            // Lets corrupt something
            using (var options = StorageEnvironmentOptions.ForPathForTests(DataDir))
            using (var pager = LinuxTestUtils.GetNewPager(options, DataDir, "Raven.Voron"))
            using (var tempTX = new TempPagerTransaction())
            {
                var writePtr = pager.AcquirePagePointer(tempTX, 2) + PageHeader.SizeOf + 43; // just some random place on page #2
                for (byte i = 0; i < 8; i++)
                {
                    writePtr[i] = i;
                }
            }

            // Now lets try to read it all back and hope we get an exception
            try
            {
                using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPathForTests(DataDir)))
                {
                    using (var tx = env.ReadTransaction())
                    {

                        foreach (var treeName in treeNames)
                        {
                            var tree = tx.CreateTree(treeName);

                            for (int i = 0; i < recordCount; i++)
                            {
                                Assert.True(tree.TryRead($"{treeName}/items/{i}", out var reader));

                                if (i % 2 == 0)
                                {
                                    var readBytes = new byte[value1.Length];
                                    reader.Read(readBytes, 0, readBytes.Length);
                                }
                                else
                                {
                                    var readBytes = new byte[value2.Length];
                                    reader.Read(readBytes, 0, readBytes.Length);
                                }
                            }
                        }
                    }

                }
            }
            catch (Exception e)
            {
                Assert.True(e is InvalidOperationException || e is InvalidDataException);
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public unsafe void ValidatePageChecksumShouldDetectCorruptionInLast64PagesOfDataFile()
        {
            // 1 MB data file = 128 pages, an exact multiple of 64, so the last 64 pages
            // share the final bitmap word that the constructor pads with "validated" bits.
            var options = StorageEnvironmentOptions.ForPathForTests(DataDir);
            options.InitialFileSize = 1024 * 1024;
            options.ManualFlushing = true;

            using (var env = new StorageEnvironment(options))
            {
                Assert.Equal(0, env._lastValidPageAfterLoad % 64);

                using (var tx = env.WriteTransaction())
                {
                    tx.CreateTree("test").Add("items/1", new byte[16]);
                    tx.Commit();
                }

                env.FlushLogToDataFile();
            }

            // Corrupt page 100 (inside the last 64 pages). No transaction ever wrote
            // it, so no journal can shadow it and the read must hit the data file.
            // Keep its header well-formed (PageNumber = 100) so the corruption
            // surfaces as a checksum mismatch.
            using (var fileStream = OpenDataFile())
            {
                long pageOffset = 100 * Constants.Storage.PageSize;
                fileStream.Position = pageOffset + (long)Marshal.OffsetOf<PageHeader>(nameof(PageHeader.PageNumber));
                fileStream.Write(BitConverter.GetBytes(100L), 0, sizeof(long));
                fileStream.Position = pageOffset + PageHeader.SizeOf + 43;
                fileStream.Write(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7 }, 0, 8);
            }

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPathForTests(DataDir)))
            {
                using (var tx = env.ReadTransaction())
                {
                    var exception = Assert.Throws<InvalidDataException>(() => tx.LowLevelTransaction.GetPage(100));
                    Assert.Contains("checksum", exception.Message);
                }
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void ValidPagesBitmapShouldOnlyMarkUnusedBitsAsValidated()
        {
            // 512 KB = 64 pages: an exact multiple of 64, so the only bitmap word
            // holds real pages exclusively and none of its bits may be pre-set.
            using (var env = CreateEnvironment(Path.Combine(DataDir, "multipleOf64"), 512 * 1024))
            {
                Assert.Equal(64, env._lastValidPageAfterLoad);
                var lastWord = env._validPagesAfterLoad[env._validPagesAfterLoad.Length - 1];
                Assert.Equal(0L, lastWord);
            }

            // 512 KB + 16 pages = 80 pages: remainder 16, so only the 48 high bits
            // of the last word (the ones beyond the last real page) may be pre-set.
            using (var env = CreateEnvironment(Path.Combine(DataDir, "remainder16"), 512 * 1024 + 16 * Constants.Storage.PageSize))
            {
                Assert.Equal(80, env._lastValidPageAfterLoad);
                var lastWord = env._validPagesAfterLoad[env._validPagesAfterLoad.Length - 1];
                Assert.Equal(unchecked((long)ulong.MaxValue << 16), lastWord);
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public unsafe void PageIsValidatedOnlyOnceThenCorruptionIsSkippedOnLaterReads()
        {
            // 1 MB file = 128 pages, so page 40 lives in the first bitmap word,
            // which never carries padding bits.
            var options = StorageEnvironmentOptions.ForPathForTests(DataDir);
            options.InitialFileSize = 1024 * 1024;

            const long pageNumber = 40;
            using (var env = new StorageEnvironment(options))
            {
                var index = pageNumber / (8 * sizeof(long));
                var bitToSet = 1L << (int)(pageNumber % (8 * sizeof(long)));

                // Craft a valid page directly in the data file: a well-formed header
                // and a matching checksum. No transaction wrote it, so no journal can
                // shadow it and reads below must hit the data file. The write goes
                // through the OS page cache, which is coherent with the mapping the
                // open environment holds.
                using (var fileStream = OpenDataFile())
                {
                    var buffer = new byte[Constants.Storage.PageSize];
                    fileStream.Position = pageNumber * Constants.Storage.PageSize;
                    fileStream.ReadExactly(buffer, 0, buffer.Length);
                    fixed (byte* ptr = buffer)
                    {
                        var header = (PageHeader*)ptr;
                        header->PageNumber = pageNumber;
                        header->Checksum = StorageEnvironment.CalculatePageChecksum(ptr, pageNumber, header->Flags, header->OverflowSize);
                    }
                    fileStream.Position = pageNumber * Constants.Storage.PageSize;
                    fileStream.Write(buffer, 0, buffer.Length);
                }

                // The first read validates the page against the data file and sets its bit.
                Assert.Equal(0, env._validPagesAfterLoad[index] & bitToSet);
                using (var tx = env.ReadTransaction())
                {
                    tx.LowLevelTransaction.GetPage(pageNumber);
                }
                Assert.NotEqual(0, env._validPagesAfterLoad[index] & bitToSet);

                // Corrupt the page bytes on disk; the page cache is coherent with the
                // environment's mapping, so the corruption is visible to it immediately.
                using (var fileStream = OpenDataFile())
                {
                    fileStream.Position = pageNumber * Constants.Storage.PageSize + PageHeader.SizeOf + 43;
                    fileStream.Write(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7 }, 0, 8);
                }

                // The second read skips validation (the bit was set by the first read)
                // and returns the corrupted bytes.
                using (var tx = env.ReadTransaction())
                {
                    var page = tx.LowLevelTransaction.GetPage(pageNumber);
                    for (byte i = 0; i < 8; i++)
                    {
                        Assert.Equal(i, page.DataPointer[43 + i]);
                    }
                }
            }
        }

        private static StorageEnvironment CreateEnvironment(string path, long initialFileSize)
        {
            var options = StorageEnvironmentOptions.ForPathForTests(path);
            options.InitialFileSize = initialFileSize;
            return new StorageEnvironment(options);
        }

        // Voron opens the data file with read/write/delete sharing on every platform, so a plain
        // stream can write beside a live environment.
        private FileStream OpenDataFile() =>
            new FileStream(Path.Combine(DataDir, Constants.DatabaseFilename),
                FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete);
    }
}
