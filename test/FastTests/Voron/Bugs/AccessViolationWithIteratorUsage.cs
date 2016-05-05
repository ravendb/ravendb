// -----------------------------------------------------------------------
//  <copyright file="AccessViolationWithIteratorUsage.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;
using Xunit;
using Voron;

namespace FastTests.Voron.Bugs
{
    public class AccessViolationWithIteratorUsage : StorageTest
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
        }

        [Fact]
        public void ShouldNotThrow()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree(  "test");

                tree.Add("items/1", new MemoryStream());
                tree.Add("items/2", new MemoryStream());

                tx.Commit();
            }

            using (var txr = Env.ReadTransaction())
            using (var iterator = txr.ReadTree("test").Iterate())
            {
                using (var tx = Env.WriteTransaction())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        tx.CreateTree( "test").Add("items/" + i, new MemoryStream(new byte[2048]));
                    }

                    tx.Commit();
                }

                Assert.True(iterator.Seek(Slices.BeforeAllKeys));

                using (var tx = Env.WriteTransaction())
                {
                    for (int i = 10; i < 40; i++)
                    {
                        tx.CreateTree("test").Add("items/" + i, new MemoryStream(new byte[2048]));
                    }

                    tx.Commit();
                }

                iterator.MoveNext();
            }
        }
    }
}