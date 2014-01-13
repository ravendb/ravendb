namespace Voron.Tests.Bugs
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;

    using Voron.Impl;
    using Voron.Trees;

    using Xunit;

    public class T1 : StorageTest
    {


        [Fact]
        public void T0()
        {
            var options = StorageEnvironmentOptions.GetInMemory();
            options.ManualFlushing = true;
            using (var env = new StorageEnvironment(options))
            {
                var trees = CreateTrees(env, 1, "tree");

                using (var txw = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    txw.Environment.State.GetTree(txw,"tree0").Add(txw, "key/1" + new string('0', 2000), new MemoryStream());
                    txw.Commit();

                    using (var txr = env.NewTransaction(TransactionFlags.Read))
                    {
                        Assert.NotNull(txr.Environment.State.GetTree(txr, "tree0").Read(txr, "key/1"));
                    }
                }
            }
        }



        [Fact]
        public void T()
        {
            var numberOfWriteThreads = 10;
            var numberOfReadThreads = 10;
            var numberOfTrees = 2;


            if (Directory.Exists("aaa"))
                Directory.Delete("aaa", true);

            //var options = StorageEnvironmentOptions.GetInMemory();
            var options = StorageEnvironmentOptions.ForPath("aaa");

            options.ManualFlushing = false;
            using (var env = new StorageEnvironment(options))
            {
                var trees = CreateTrees(env, numberOfTrees, "tree");

                var t1 = Task.Factory.StartNew(
                    () =>
                    {
                        Parallel.For(
                            0,
                            numberOfWriteThreads,
                            i =>
                            {
                                var random = new Random(i ^ 1337);
                                var dataSize = random.Next(100, 100);
                                var buffer = new byte[dataSize];
                                random.NextBytes(buffer);
                                while (true)
                                {
                                    var tIndex = random.Next(0, numberOfTrees - 1);
                                    var treeName = trees[tIndex];

                                    var batch = new WriteBatch();
                                    batch.Add("testdocuments/" + random.Next(0, 100000), new MemoryStream(buffer), treeName);

                                    try
                                    {
                                        env.Writer.Write(batch);
                                    }
                                    catch (Exception)
                                    {
                                    }

                                    //env.FlushLogToDataFile();
                                }
                            });
                    },
                    TaskCreationOptions.LongRunning);

                var t2 = Task.Factory.StartNew(
                    () =>
                    {
                        Parallel.For(
                            0,
                            numberOfReadThreads,
                            i =>
                            {
                                var random = new Random(i);

                                while (true)
                                {
                                    var tIndex = random.Next(0, numberOfTrees - 1);
                                    var treeName = trees[tIndex];

                                    using (var snapshot = env.CreateSnapshot())
                                    using (var iterator = snapshot.Iterate(treeName))
                                    {
                                        if (!iterator.Seek(Slice.BeforeAllKeys))
                                        {
                                            continue;
                                        }

                                        do
                                        {
                                            try
                                            {
                                                Assert.Contains("testdocuments/", iterator.CurrentKey.ToString());
                                            }
                                            catch (Exception)
                                            {
                                                RenderAndShow(env, treeName);
                                            }

                                        }
                                        while (iterator.MoveNext());
                                    }
                                }
                            });
                    },
                    TaskCreationOptions.LongRunning);

                Task.WaitAll(new Task[] { t1, t2 }, TimeSpan.FromSeconds(100));
            }
        }

        private void RenderAndShow(StorageEnvironment env, string treeName)
        {
            using (var tx = env.NewTransaction(TransactionFlags.Read))
            {
                RenderAndShow(tx, 1, treeName);
            }
        }
    }
}