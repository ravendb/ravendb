using System;
using System.IO;
using System.IO.Compression;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Voron;
using Voron.Data.BTrees;
using Voron.Impl.FileHeaders;
using Voron.Impl.Journal;
using Xunit;
using Xunit.Abstractions;
using Hashing = Sparrow.Hashing;

namespace FastTests.Voron.SharedJournal;

public class LegacyHandling(ITestOutputHelper output) : RavenTestBase(output)
{
    /*
     All the data in this file is using a Legacy-db.zip file that was genearte
     using RavenDB 6.2 using the following code
     
     using (var env = new StorageEnvironment(options))
     {
         for (int i = 0; i < 10; i++)
         {
             using (var tx = env.WriteTransaction())
             {
                 var tree = tx.CreateTree("legacy-tree");
                 tree.Add(i.ToString(), (i + 100).ToString());
                 tx.Commit();
             }
         }
     }
     */
    [RavenFact(RavenTestCategory.Voron)]
    public void CanHandleStartingWithLegacyDbAsRoot()
    {
        string newDataPath = NewDataPath();
        IOExtensions.DeleteDirectory(newDataPath);
        using var stream = typeof(LegacyHandling).Assembly.GetManifestResourceStream(typeof(LegacyHandling).Namespace + ".Legacy-db.zip");
        using var zipArchive = new ZipArchive(stream, ZipArchiveMode.Read);
        zipArchive.ExtractToDirectory(newDataPath);

        {
            var options = StorageEnvironmentOptions.ForPathForTests(newDataPath);
            options.ManualFlushing = true;
            using var env = new StorageEnvironment(options);
        
            using (var txr = env.ReadTransaction())
            {
                Tree tree = txr.ReadTree("legacy-tree");
                Assert.Equal(10, tree.ReadHeader().NumberOfEntries);

                for (int i = 0; i < 10; i++)
                {
                    Assert.Equal((i+100).ToString(),
                        tree.Read(i.ToString()).Reader.ToStringValue());
                }
            }

            var header = env.HeaderAccessor.CopyHeader();
            Assert.Equal(env.DbId, header.DatabaseId);
        
            using (var txw = env.WriteTransaction())
            {
                var tree = txw.ReadTree("legacy-tree");
                tree.Add("after", "works");
                txw.Commit();
            }
        }

        {
            var options = StorageEnvironmentOptions.ForPathForTests(newDataPath);
            using var env = new StorageEnvironment(options);

            using (var txr = env.ReadTransaction())
            {
                Tree tree = txr.ReadTree("legacy-tree");
                Assert.Equal(11, tree.ReadHeader().NumberOfEntries);
                Assert.Equal("works", tree.Read("after").Reader.ToStringValue());
                for (int i = 0; i < 10; i++)
                {
                    Assert.Equal((i+100).ToString(),
                        tree.Read(i.ToString()).Reader.ToStringValue());
                }
            }
        }
    }
    
    [RavenFact(RavenTestCategory.Voron)]
    public void ShouldNotUseLegacyJournalForNewWrites()
    {
        string newDataPath = NewDataPath();
        IOExtensions.DeleteDirectory(newDataPath);
        using var stream = typeof(LegacyHandling).Assembly.GetManifestResourceStream(typeof(LegacyHandling).Namespace + ".Legacy-db.zip");
        using var zipArchive = new ZipArchive(stream, ZipArchiveMode.Read);
        zipArchive.ExtractToDirectory(newDataPath);

        {
            var options = StorageEnvironmentOptions.ForPathForTests(newDataPath);
            using var env = new StorageEnvironment(options);
            
            // The old journal file has no TransactionId marking, so all are Guid.Empty
            // if we re-use that across multiple env, we may apply that to all of them
            // so we need to ensure that we do _not_ reuse the journal file if any
            // transaction in it has Guid.Empty in the DatabaseId
            
            Assert.Null(env.Journal.CurrentFile);
            
            using (var txw = env.WriteTransaction())
            {
                var tree = txw.ReadTree("legacy-tree");
                tree.Add("after", "works");
                txw.Commit();
            }
            Assert.Equal(1, env.Journal.CurrentFile.Number);
        }
    }
    
    [RavenFact(RavenTestCategory.Voron)]
    public void CanHandleStartingWithLegacyDbAsBranch()
    {
        string legacyPath = NewDataPath();
        IOExtensions.DeleteDirectory(legacyPath);
        using var stream = typeof(LegacyHandling).Assembly.GetManifestResourceStream(typeof(LegacyHandling).Namespace + ".Legacy-db.zip");
        using var zipArchive = new ZipArchive(stream, ZipArchiveMode.Read);
        zipArchive.ExtractToDirectory(legacyPath);

        string rootPath = NewDataPath();
        IOExtensions.DeleteDirectory(rootPath);
        var optionsForRoot = StorageEnvironmentOptions.ForPathForTests(rootPath);
        optionsForRoot.ManualFlushing = true;
        using var root = new StorageEnvironment(optionsForRoot);
        using var _ = root.Journal.SharedJournalsScope(CancellationToken.None);
        {
            using var branchOptions = StorageEnvironmentOptions.ForPathForTests(legacyPath);
            branchOptions.RootJournal = root.Journal;
            branchOptions.ManualFlushing = true;
            using var branch = new StorageEnvironment(branchOptions);
        
            using (var txr = branch.ReadTransaction())
            {
                Tree tree = txr.ReadTree("legacy-tree");
                Assert.Equal(10, tree.ReadHeader().NumberOfEntries);

                for (int i = 0; i < 10; i++)
                {
                    Assert.Equal((i+100).ToString(),
                        tree.Read(i.ToString()).Reader.ToStringValue());
                }
            }

            var header = branch.HeaderAccessor.CopyHeader();
            Assert.Equal(branch.DbId, header.DatabaseId);
        
            var mre = new ManualResetEventSlim(false);
            root.Journal.OnBranchJournalEntrySubmitted += () =>
            {
                mre.Set();
            };
            // Now do another write
            var task = Task.Run(() =>
            {
                Output.WriteLine(branch.DbId.ToString());
                using (var branchTx = branch.WriteTransaction())
                {
                    Tree tree = branchTx.CreateTree("legacy-tree");
                    tree.Add("after", "works");
                    branchTx.Commit();
                }
            }).ContinueWith(_ => mre.Set());

            SharedJournalTests.WaitForTaskAndExecuteBranchTransactions(task, mre, root);
        }
        
        {
            var options = StorageEnvironmentOptions.ForPathForTests(legacyPath);
            options.RootJournal = root.Journal;
            options.ManualFlushing = true;
            
            using var branch = new StorageEnvironment(options);
            using (var txr = branch.ReadTransaction())
            {
                Tree tree = txr.ReadTree("legacy-tree");
                Assert.Equal(11, tree.ReadHeader().NumberOfEntries);
                Assert.Equal("works", tree.Read("after").Reader.ToStringValue());
                for (int i = 0; i < 10; i++)
                {
                    Assert.Equal((i+100).ToString(),
                        tree.Read(i.ToString()).Reader.ToStringValue());
                }
            }
        }
    }
    
    [RavenFact(RavenTestCategory.Voron)]
    public void CanHandleRecycledJournals()
    {
        string newDataPath = NewDataPath();
        IOExtensions.DeleteDirectory(newDataPath);
        using var stream = typeof(LegacyHandling).Assembly.GetManifestResourceStream(typeof(LegacyHandling).Namespace + ".Legacy-db.zip");
        using var zipArchive = new ZipArchive(stream, ZipArchiveMode.Read);
        zipArchive.ExtractToDirectory(newDataPath);
        // This creates a "recycled journal" that we need to properly handle during recovery
        File.Copy(
            Path.Combine(newDataPath, "Journals/0000000000000000000.journal"),
            Path.Combine(newDataPath, "Journals/0000000000000000001.journal")
            );
        
        // truncate the file so it won't include the _last_ transaction
        using(var f = File.Open(Path.Combine(newDataPath, "Journals/0000000000000000001.journal"), FileMode.Open))
        {
            f.SetLength(f.Length/2);
        }

        UpdateHeaderToAddJournal(newDataPath);

        {
            var options = StorageEnvironmentOptions.ForPathForTests(newDataPath);
            options.ManualFlushing = true;
            using var env = new StorageEnvironment(options);
        
            using (var txr = env.ReadTransaction())
            {
                Tree tree = txr.ReadTree("legacy-tree");
                Assert.Equal(10, tree.ReadHeader().NumberOfEntries);

                for (int i = 0; i < 10; i++)
                {
                    Assert.Equal((i+100).ToString(),
                        tree.Read(i.ToString()).Reader.ToStringValue());
                }
            }

            var header = env.HeaderAccessor.CopyHeader();
            Assert.Equal(env.DbId, header.DatabaseId);
        
            using (var txw = env.WriteTransaction())
            {
                var tree = txw.ReadTree("legacy-tree");
                tree.Add("after", "works");
                txw.Commit();
            }
        }

        {
            var options = StorageEnvironmentOptions.ForPathForTests(newDataPath);
            options.ManualFlushing = true;
            using var env = new StorageEnvironment(options);

            using (var txr = env.ReadTransaction())
            {
                Tree tree = txr.ReadTree("legacy-tree");
                Assert.Equal(11, tree.ReadHeader().NumberOfEntries);
                Assert.Equal("works", tree.Read("after").Reader.ToStringValue());
                for (int i = 0; i < 10; i++)
                {
                    Assert.Equal((i+100).ToString(),
                        tree.Read(i.ToString()).Reader.ToStringValue());
                }
            }
        }
    }

    private static void UpdateHeaderToAddJournal(string newDataPath)
    {
        Span<byte> headerOne = File.ReadAllBytes(Path.Combine(newDataPath, "headers.one"));
        int journalOffset = Marshal.OffsetOf<FileHeader>(nameof(FileHeader.Journal)).ToInt32();
        int revisionOffset = Marshal.OffsetOf<FileHeader>(nameof(FileHeader.HeaderRevision)).ToInt32();
        int transactionIdOffset = Marshal.OffsetOf<FileHeader>(nameof(FileHeader.TransactionId)).ToInt32();
        var journalInfo = MemoryMarshal.Read<JournalInfo>(headerOne[journalOffset..]);
        journalInfo.CurrentJournal = 1;
        MemoryMarshal.Write(headerOne[journalOffset..], journalInfo);
        long revision = MemoryMarshal.Read<long>(headerOne[revisionOffset..]);
        MemoryMarshal.Write(headerOne[revisionOffset..], revision + 10);
        MemoryMarshal.Write(headerOne[^sizeof(long)..], 
            Hashing.XXHash64.CalculateInline(headerOne[..^sizeof(ulong)], 
                MemoryMarshal.Read<ulong>(headerOne[transactionIdOffset..]))
        );
        File.WriteAllBytes(Path.Combine(newDataPath, "headers.one"), headerOne.ToArray());
    }
}
