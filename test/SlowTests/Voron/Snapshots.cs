using System;
using System.IO;
using System.Linq;
using FastTests.Voron;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron
{
    public class Snapshots : StorageTest
    {
        public Snapshots(ITestOutputHelper output)
            : base(StorageEnvironmentOptions.CreateMemoryOnlyForTests(), output)
        {

        }

        [Fact]
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
                    var result = snapshot.CreateTree("tree1").Read("docs/" + i);
                    Assert.NotNull(result);

                    {
                        var bytes = result.Reader.ReadBytes(result.Reader.Length);
                        Assert.Equal(testBuffer, bytes.Array.Skip(bytes.Offset).Take(bytes.Count).ToArray());

                    }
                }
            }
        }

        [Fact]
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
                    var result = snapshot.ReadTree("tree1").Read("docs/" + i);
                    Assert.NotNull(result);

                    {
                        var bytes = result.Reader.ReadBytes(result.Reader.Length);
                        Assert.Equal(testBuffer, bytes.Array.Skip(bytes.Offset).Take(bytes.Count).ToArray());
                    }
                }
            }
        }
    }
}
