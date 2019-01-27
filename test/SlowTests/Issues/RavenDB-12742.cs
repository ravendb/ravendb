using System;
using System.Collections.Generic;
using System.Text;
using FastTests;
using FastTests.Voron;
using Voron.Data.BTrees;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12742 : StorageTest
    {
        [Fact]
        public void CanOverwriteBigValueInSameTx()
        {
            var r = new Random();
            var bytes = new byte[32 * 1024];

            using (var tx = Env.WriteTransaction())
            {
                Tree tree = tx.CreateTree("tree");

                r.NextBytes(bytes);
                tree.Add("key", bytes);

                r.NextBytes(bytes);
                tree.Add("key", bytes);

                tx.Commit();
            }
        }
    }
}
