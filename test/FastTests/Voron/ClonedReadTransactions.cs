using System;
using System.Collections.Generic;
using System.Text;
using Sparrow.Server.Platform;
using Voron;
using Voron.Data.BTrees;
using Voron.Impl.Paging;
using Xunit;

namespace FastTests.Voron
{
    public class ClonedReadTransactions : StorageTest
    {
        [Fact]
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
