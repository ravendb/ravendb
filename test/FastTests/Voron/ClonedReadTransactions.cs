using Voron;
using Voron.Data.BTrees;
using Xunit;
using Tests.Infrastructure;

namespace FastTests.Voron
{
    public class ClonedReadTransactions : StorageTest
    {
        public ClonedReadTransactions(ITestOutputHelper output) : base(output)
        {
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
                    Assert.True(outer.CreateTree("test").TryRead("hello", out var reader));
                    var result = reader.ReadString(reader.Length);
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
                        Assert.True(tree.TryRead("hello", out var reader));
                        var result = reader.ReadString(reader.Length);

                        Assert.Equal("one", result);
                    }

                    using (var finalRead = Env.ReadTransaction())
                    {
                        Assert.True(finalRead.CreateTree("test").TryRead("hello", out var reader));
                        var result = reader.ReadString(reader.Length);

                        Assert.Equal("three", result);
                    }
                }
            }


        }
    }
}
