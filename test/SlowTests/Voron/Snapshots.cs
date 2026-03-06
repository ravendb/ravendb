using System;
using System.IO;
using System.Linq;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace SlowTests.Voron
{
    public class Snapshots : StorageTest
    {
        public Snapshots(ITestOutputHelper output)
            : base(StorageEnvironmentOptions.CreateMemoryOnlyForTests(), output)
        {

        }

        [RavenFact(RavenTestCategory.Voron)]
        public void SnapshotIssue()
        {
            const int DocumentCount = 50000;

            var rand = new Random();
            var testBuffer = new byte[39];
            rand.NextBytes(testBuffer);


            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree1");
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var t1 = tx.CreateTree("tree1");
                for (var i = 0; i < DocumentCount; i++)
                {
                    t1.Add("docs/" + i, new MemoryStream(testBuffer));
                }

                tx.Commit();
            }

            using (var snapshot = Env.ReadTransaction())
            {
                using (var tx = Env.WriteTransaction())
                {
                    var t1 = tx.CreateTree("tree1");
                    for (var i = 0; i < DocumentCount; i++)
                    {
                        t1.Delete("docs/" + i);
                    }

                    tx.Commit();
                }

                for (var i = 0; i < DocumentCount; i++)
                {
                    Assert.True(snapshot.CreateTree("tree1").TryRead("docs/" + i, out var reader));
                    
                    var bytes = reader.ReadBytes(reader.Length);
                    Assert.Equal(testBuffer, bytes.Array.Skip(bytes.Offset).Take(bytes.Count).ToArray());
                }
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void SnapshotIssue_ExplicitFlushing()
        {
            const int DocumentCount = 50000;

            var rand = new Random();
            var testBuffer = new byte[39];
            rand.NextBytes(testBuffer);

            Options.ManualFlushing = true;

            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree1");
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var t1 = tx.CreateTree("tree1");
                for (var i = 0; i < DocumentCount; i++)
                {
                    t1.Add("docs/" + i, new MemoryStream(testBuffer));
                }

                tx.Commit();
            }

            Env.FlushLogToDataFile();

            using (var snapshot = Env.ReadTransaction())
            {
                using (var tx = Env.WriteTransaction())
                {
                    var t1 = tx.CreateTree("tree1");
                    for (var i = 0; i < DocumentCount; i++)
                    {
                        t1.Delete("docs/" + i);
                    }

                    tx.Commit();
                }

                Env.FlushLogToDataFile();

                for (var i = 0; i < DocumentCount; i++)
                {
                    Assert.True(snapshot.ReadTree("tree1").TryRead("docs/" + i, out var reader));

                    var bytes = reader.ReadBytes(reader.Length);
                    Assert.Equal(testBuffer, bytes.Array.Skip(bytes.Offset).Take(bytes.Count).ToArray());
                }
            }
        }
    }
}
