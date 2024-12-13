using System.IO;
using System.IO.Compression;
using Tests.Infrastructure;
using Voron;
using Voron.Data.BTrees;
using Voron.Impl.FileHeaders;
using Xunit;
using Xunit.Abstractions;

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
        using var stream = typeof(LegacyHandling).Assembly.GetManifestResourceStream(typeof(LegacyHandling).Namespace + ".Legacy-db.zip");
        using var zipArchive = new ZipArchive(stream, ZipArchiveMode.Read);
        zipArchive.ExtractToDirectory(newDataPath);

        using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPathForTests(newDataPath)))
        {
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
        }
    }
}
