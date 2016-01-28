using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Voron;
using Voron.Data.Compact;
using Xunit;

namespace FastTests.Voron.Compact
{
    public class SimplePrefixTree : PrefixTreeStorageTests
    {
        private string Name = "MyTree";

        [Fact]
        public void Construction()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreatePrefixTree(Name);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadPrefixTree(Name);

                Assert.Equal(0, tree.Count);
                Assert.Equal(Slice.BeforeAllKeys, tree.FirstKeyOrDefault());
                Assert.Equal(Slice.AfterAllKeys, tree.LastKeyOrDefault());

                StructuralVerify(tree);
            }
        }

        [Fact]
        public void Operations_SingleElement_Invariants()
        {
            var key = "oren";

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreatePrefixTree(Name);

                Assert.True(tree.Add(key, (Slice)"eini"));

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadPrefixTree(Name);

                Assert.Equal(key, tree.FirstKey());
                Assert.Equal(key, tree.LastKey());
                Assert.True(tree.Contains(key));

                string value;
                Assert.True(tree.TryGet(key, out value));

                StructuralVerify(tree);
            }
        }

        [Fact]
        public void Operations_SingleElement_Operations()
        {
            var key = "oren";

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreatePrefixTree(Name);

                Assert.True(tree.Add(key, (Slice)"eini"));

                StructuralVerify(tree);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadPrefixTree(Name);

                StructuralVerify(tree);

                // x+ = min{y ? S | y = x} (the successor of x in S) - Page 160 of [1]
                // Therefore the successor of the key "oren" is greater or equal to "oren"
                Assert.Equal(key, tree.Successor(key));
                Assert.Equal(Slice.AfterAllKeys, tree.Successor("qu"));

                // x- = max{y ? S | y < x} (the predecessor of x in S) - Page 160 of [1] 
                // Therefore the predecessor of the key "oren" is strictly less than "oren".
                Assert.Equal(Slice.BeforeAllKeys, tree.Predecessor(key));
                Assert.Equal(Slice.BeforeAllKeys, tree.Predecessor("aq"));
                Assert.Equal(key, tree.Predecessor("pq"));


            }
        }

    }
}
