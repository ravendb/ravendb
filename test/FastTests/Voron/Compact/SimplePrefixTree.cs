using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Voron;
using Voron.Data.Compact;
using Voron.Data.Tables;
using Xunit;

namespace FastTests.Voron.Compact
{
    public class SimplePrefixTree : PrefixTreeStorageTests
    {
        private string Name = "MyTree";

        private void InitializeStorage()
        {
            using (var tx = Env.WriteTransaction())
            {
                DocsSchema.Create(tx, "docs");
                tx.CreatePrefixTree(Name);

                tx.Commit();
            }
        }


        [Fact]
        public void Construction()
        {
            InitializeStorage();

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadPrefixTree(Name);

                Assert.Equal(0, tree.Count);
                Assert.Equal(Slice.BeforeAllKeys, tree.FirstKey());
                Assert.Equal(Slice.AfterAllKeys, tree.LastKey());

                StructuralVerify(tree);
            }
        }

        [Fact]
        public void Operations_SingleElement_Invariants()
        {
            InitializeStorage();

            Slice key = new Slice(Encoding.UTF8.GetBytes("oren"));

            long recordId;
            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = tx.CreatePrefixTree(Name);

                recordId = SetHelper(docs, key, "eini");
                Assert.True(tree.Add(key, recordId));

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadPrefixTree(Name);

                var docs = new Table(DocsSchema, "docs", tx);
                docs.ReadByKey(key);

                Assert.Equal(key, tree.FirstKey());
                Assert.Equal(key, tree.LastKey());
                Assert.True(tree.Contains(key));

                long value;
                Assert.True(tree.TryGet(key, out value));
                Assert.Equal(recordId, value);

                StructuralVerify(tree);
            }
        }

        [Fact]
        public void Operations_SingleElement_Operations()
        {
            InitializeStorage();

            Slice key = new Slice(Encoding.UTF8.GetBytes("oren"));

            long recordId;
            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = tx.CreatePrefixTree(Name);

                recordId = SetHelper(docs, key, "eini");
                Assert.True(tree.Add(key, recordId));

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

        private long AddToPrefixTree(PrefixTree tree, Table table, Slice key, string value)
        {
            long recordId = SetHelper(table, key, value);
            Assert.True(tree.Add(key, recordId));
            return recordId;
        }

        [Fact]
        public void Operations_SingleBranchInsertion()
        {
            InitializeStorage();

            Slice smallestKey = new Slice(Encoding.UTF8.GetBytes("Ar"));
            Slice lesserKey = new Slice(Encoding.UTF8.GetBytes("Oren")); 
            Slice greaterKey = new Slice(Encoding.UTF8.GetBytes("oren")); 
            Slice greatestKey = new Slice(Encoding.UTF8.GetBytes("zz"));

            long lesserKeyRecordId = -1;
            long greaterKeyRecordId = -1;
            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = tx.CreatePrefixTree(Name);

                AddToPrefixTree(tree, docs, lesserKey, "Oren");
                AddToPrefixTree(tree, docs, greaterKey, "Eini");

                StructuralVerify(tree);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadPrefixTree(Name);
                StructuralVerify(tree);

                Assert.Equal(lesserKey, tree.FirstKey());
                Assert.Equal(greaterKey, tree.LastKey());

                Assert.True(tree.Contains(greaterKey));
                Assert.True(tree.Contains(lesserKey));

                long value;
                Assert.True(tree.TryGet(lesserKey, out value));
                Assert.Equal(lesserKeyRecordId, value);

                Assert.True(tree.TryGet(greaterKey, out value));
                Assert.Equal(greaterKeyRecordId, value);

                Assert.False(tree.TryGet(greaterKey + "1", out value));
                Assert.Equal(-1, value);

                Assert.False(tree.TryGet("1", out value));
                Assert.Equal(-1, value);

                // x+ = min{y ? S | y = x} (the successor of x in S) - Page 160 of [1]
                // Therefore the successor of the key "oren" is greater or equal to "oren"
                Assert.Equal(lesserKey, tree.Successor(lesserKey));
                Assert.Equal(greaterKey, tree.Successor(greaterKey));
                Assert.Equal(greaterKey, tree.Successor(lesserKey + "1"));
                Assert.Equal(Slice.AfterAllKeys, tree.Successor(greatestKey));

                // x- = max{y ? S | y < x} (the predecessor of x in S) - Page 160 of [1] 
                // Therefore the predecessor of the key "oren" is strictly less than "oren".
                Assert.Equal(lesserKey, tree.Predecessor(greaterKey));
                Assert.Equal(Slice.BeforeAllKeys, tree.Predecessor(lesserKey));
                Assert.Equal(Slice.BeforeAllKeys, tree.Predecessor(smallestKey));
            }
        }

    }
}
