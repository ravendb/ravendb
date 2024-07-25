using Tests.Infrastructure;
using Voron;
using Voron.Data.BTrees;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron
{
    public class ClonedReadTransactions : StorageTest
    {
        public ClonedReadTransactions(ITestOutputHelper output) : base(output)
        {
        }


        [RavenFact(RavenTestCategory.Voron)]
        public unsafe void CanCloneAndReadOldDataFromReadTx_ManualFlush()
        {
            Options.ForceUsing32BitsPager = true;
            Options.ManualFlushing = true;

            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("test").Add("hello", "one");
                tx.Commit();
            }

            Env.BackgroundFlushWritesToDataFile();

            using (var outer = Env.ReadTransaction())
            {
                using (var tx = Env.WriteTransaction())
                {
                    tx.CreateTree("test").Add("hello", "two");
                    tx.Commit();
                }

                Env.BackgroundFlushWritesToDataFile();
                {
                    ValueReader readResultReader = outer.CreateTree("test").Read("hello").Reader;
                    var result = readResultReader.ReadString(readResultReader.Length);

                    Assert.Equal("one", result);
                }


                using (var inner = Env.CloneReadTransaction(outer))
                {
                    outer.Dispose();

                    using (var tx = Env.WriteTransaction())
                    {
                        tx.CreateTree("test").Add("hello", "three");
                        tx.Commit();
                    }

                    Env.BackgroundFlushWritesToDataFile();
                    {
                        Tree tree = inner.CreateTree("test");
                        ValueReader readResultReader = tree.Read("hello").Reader;
                        var result = readResultReader.ReadString(readResultReader.Length);

                        Assert.Equal("one", result);
                    }

                    using (var finalRead = Env.ReadTransaction())
                    {
                        ValueReader readResultReader = finalRead.CreateTree("test").Read("hello").Reader;
                        var result = readResultReader.ReadString(readResultReader.Length);

                        Assert.Equal("three", result);
                    }
                }
            }


        }

        [RavenFact(RavenTestCategory.Voron)]
        public unsafe void CanCloneAndReadOldDataFromReadTx()
        {
            Options.ForceUsing32BitsPager = true;


            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("test").Add("hello", "one");
                tx.Commit();
            }

            using (var outer = Env.ReadTransaction())
            {
                using (var tx = Env.WriteTransaction())
                {
                    tx.CreateTree("test").Add("hello", "two");
                    tx.Commit();
                }

                {
                    ValueReader readResultReader = outer.CreateTree("test").Read("hello").Reader;
                    var result = readResultReader.ReadString(readResultReader.Length);

                    Assert.Equal("one", result);
                }


                using (var inner = Env.CloneReadTransaction(outer))
                {
                    outer.Dispose();

                    using (var tx = Env.WriteTransaction())
                    {
                        tx.CreateTree("test").Add("hello", "three");
                        tx.Commit();
                    }

                    {
                        Tree tree = inner.CreateTree("test");
                        ValueReader readResultReader = tree.Read("hello").Reader;
                        var result = readResultReader.ReadString(readResultReader.Length);

                        Assert.Equal("one", result);
                    }

                    using (var finalRead = Env.ReadTransaction())
                    {
                        ValueReader readResultReader = finalRead.CreateTree("test").Read("hello").Reader;
                        var result = readResultReader.ReadString(readResultReader.Length);

                        Assert.Equal("three", result);
                    }
                }
            }
        }
    }
}
