// -----------------------------------------------------------------------
//  <copyright file="StartsWithSearch.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Voron.Debugging;
using Xunit;

namespace Voron.Tests.Bugs
{
    public class StartsWithSearch
    {
        [Fact]
        public void ShouldWork()
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
            {
                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var s = new string('0', 500);
                    var tree = env.CreateTree(tx, "data");
                    for (int i = 0; i < 10; i++)
                    {
                        tree.Add("users-" + i + "-" + s, new byte[0]);
                    }
                    tx.Commit();
                }

                using (var tx = env.NewTransaction(TransactionFlags.Read))
                {
                    var tree = tx.State.GetTree(tx, "data");
                    using (var it = tree.Iterate())
                    {
                        Assert.True(it.Seek("users-7"));

                        for (int i = 0; i < 10; i++)
                        {
                            Assert.True(it.Seek("users-"+i),i.ToString());
                        }
                    }
                }
            }
        }
    }
}