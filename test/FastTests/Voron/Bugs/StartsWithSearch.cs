// -----------------------------------------------------------------------
//  <copyright file="StartsWithSearch.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Sparrow.Logging;
using Xunit;
using Voron;

namespace FastTests.Voron.Bugs
{
    public class StartsWithSearch
    {
        [Fact]
        public void ShouldWork()
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
            {
                using (var tx = env.WriteTransaction())
                {
                    var s = new string('0', 500);
                    var tree = tx.CreateTree( "data");
                    for (int i = 0; i < 10; i++)
                    {
                        tree.Add("users-" + i + "-" + s, new byte[0]);
                    }
                    tx.Commit();
                }

                using (var tx = env.ReadTransaction())
                {
                    var tree = tx.ReadTree("data");
                    using (var it = tree.Iterate(false))
                    {
                        Slice key;
                        Slice.From(tx.Allocator, "users-7", out key);
                        Assert.True(it.Seek(key));

                        for (int i = 0; i < 10; i++)
                        {
                            Slice.From(tx.Allocator, "users-" + i, out key);
                            Assert.True(it.Seek(key),i.ToString());
                        }
                    }
                }
            }
        }
    }
}
