using System;
using FastTests.Voron;
using Voron.Data.BTrees;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_12742 : StorageTest
    {
        public RavenDB_12742(ITestOutputHelper output) : base(output)
        {
        }

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


        [Fact]
        public void CanOverwriteBigValueInSameTx_Decrease()
        {
            var r = new Random();

            using (var tx = Env.WriteTransaction())
            {
                Tree tree = tx.CreateTree("tree");

                var bytes = new byte[32 * 1024];
                r.NextBytes(bytes);
                tree.Add("key", bytes);

                bytes = new byte[16 * 1024];
                r.NextBytes(bytes);
                tree.Add("key", bytes);

                tx.Commit();
            }
        }

        [Fact]
        public void CanOverwriteBigValueInSameTx_Increase()
        {
            var r = new Random();

            using (var tx = Env.WriteTransaction())
            {
                Tree tree = tx.CreateTree("tree");

                var bytes = new byte[32 * 1024];
                r.NextBytes(bytes);
                tree.Add("key", bytes);

                bytes = new byte[48 * 1024];
                r.NextBytes(bytes);
                tree.Add("key", bytes);

                tx.Commit();
            }
        }
    }
}
