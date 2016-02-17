using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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


        [Fact(Skip = "Cause an assert failure right now")]
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

        [Fact(Skip = "Cause an assert failure right now")]
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

        [Fact(Skip = "Cause an assert failure right now")]
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

        private long AddToPrefixTree(PrefixTree tree, Table table, string key, string value)
        {
            return AddToPrefixTree(tree, table, new Slice(Encoding.UTF8.GetBytes(key)), value);
        }

        private long AddToPrefixTree(PrefixTree tree, Table table, Slice key, string value)
        {
            long recordId = SetHelper(table, key, value);
            Assert.True(tree.Add(key, recordId));
            return recordId;
        }

        [Fact(Skip = "Cause an assert failure right now")]
        public void Structure_MultipleBranchInsertion()
        {
            InitializeStorage();

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = tx.CreatePrefixTree(Name);

                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "8Jp3", "8Jp3"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "GX37", "GX37"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "f04o", "f04o"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "KmGx", "KmGx"));

                StructuralVerify(tree);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadPrefixTree(Name);
                StructuralVerify(tree);

                Assert.Equal(4, tree.Count);
            }
        }

        [Fact(Skip = "Cause an assert failure right now")]
        public void Structure_MultipleBranch_OrderPreservation()
        {
            InitializeStorage();

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = tx.CreatePrefixTree(Name);

                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "8Jp3", "8Jp3"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "V6sl", "V6sl"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "GX37", "GX37"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "f04o", "f04o"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "KmGx", "KmGx"));

                StructuralVerify(tree);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadPrefixTree(Name);
                StructuralVerify(tree);

                Assert.Equal(5, tree.Count);
            }
        }
    }
}
