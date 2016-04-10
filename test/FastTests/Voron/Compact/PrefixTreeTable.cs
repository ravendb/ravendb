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
    public class PrefixTreeTable : PrefixTreeStorageTests
    {
        [Fact]
        public void Construction()
        {
            InitializeStorage();

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

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
                recordId = SetHelper(docs, key, "eini");

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                var reader = docs.ReadByKey(key);                

                Assert.Equal(key, tree.FirstKey());
                Assert.Equal(key, tree.LastKey());
                Assert.True(tree.Contains(key));

                long value;
                Assert.True(tree.TryGet(key, out value));
                Assert.Equal(recordId, value);
                Assert.Equal(reader.Id, value);

                StructuralVerify(tree);
            }
        }

        [Fact]
        public void Operations_SingleElement_Operations()
        {
            InitializeStorage();

            Slice key = new Slice(Encoding.UTF8.GetBytes("oren"));

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                SetHelper(docs, key, "eini");

                StructuralVerify(tree);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

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

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                Assert.True(tree.Delete(key));
                Assert.False(tree.Delete(key));

                Assert.Equal(0, tree.Count);
                Assert.Equal(Slice.BeforeAllKeys, tree.FirstKey());
                Assert.Equal(Slice.AfterAllKeys, tree.LastKey());

                StructuralVerify(tree);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                Assert.Equal(0, tree.Count);
                Assert.False(tree.Contains(key));

                Assert.Equal(Slice.BeforeAllKeys, tree.FirstKey());
                Assert.Equal(Slice.AfterAllKeys, tree.LastKey());

                StructuralVerify(tree);
            }
        }

        [Fact]
        public void Structure_MultipleBranchInsertion()
        {
            InitializeStorage();

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "8Jp3", "8Jp3"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "GX37", "GX37"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "f04o", "f04o"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "KmGx", "KmGx"));

                StructuralVerify(tree);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                StructuralVerify(tree);

                Assert.Equal(4, tree.Count);
            }
        }

        [Fact]
        public void Structure_MultipleBranchDeletion()
        {
            InitializeStorage();

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "8Jp3", "8Jp3"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "GX37", "GX37"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "f04o", "f04o"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "KmGx", "KmGx"));

                StructuralVerify(tree);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                docs.DeleteByKey(new Slice(Encoding.UTF8.GetBytes("8Jp3")));
                docs.DeleteByKey(new Slice(Encoding.UTF8.GetBytes("GX37")));
                docs.DeleteByKey(new Slice(Encoding.UTF8.GetBytes("f04o")));
                docs.DeleteByKey(new Slice(Encoding.UTF8.GetBytes("KmGx")));

                StructuralVerify(tree);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                Assert.Equal(0, tree.Count);

                StructuralVerify(tree);
            }
        }

        [Fact]
        public void Structure_MultipleBranchInsertion2()
        {
            InitializeStorage();

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "X2o", "X2o"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "DWp", "DWp"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "C70", "C70"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "1b5", "1b5"));

                StructuralVerify(tree);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                StructuralVerify(tree);

                Assert.Equal(4, tree.Count);
            }
        }

        [Fact]
        public void Structure_MultipleBranchDeletion2()
        {
            InitializeStorage();

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "X2o", "X2o"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "DWp", "DWp"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "C70", "C70"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "1b5", "1b5"));

                StructuralVerify(tree);

                tx.Commit();
            }


            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                docs.DeleteByKey(new Slice(Encoding.UTF8.GetBytes("1b5")));
                docs.DeleteByKey(new Slice(Encoding.UTF8.GetBytes("DWp")));
                docs.DeleteByKey(new Slice(Encoding.UTF8.GetBytes("X2o")));
                docs.DeleteByKey(new Slice(Encoding.UTF8.GetBytes("C70")));

                StructuralVerify(tree);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                Assert.Equal(0, tree.Count);

                StructuralVerify(tree);
            }
        }

        [Fact]
        public void Structure_MultipleBranchInsertion3()
        {
            InitializeStorage();

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "0Ji", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "gBx", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "UIc", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "Zey", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "DV2", string.Empty));

                StructuralVerify(tree);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                StructuralVerify(tree);

                Assert.Equal(5, tree.Count);
            }
        }

        [Fact]
        public void Structure_MultipleBranch_InternalExtent()
        {
            InitializeStorage();

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "8Jp3V6sl", "8Jp3"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "VJ7hXe8d", "V6sl"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "39XCGX37", "GX37"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "f04oKmGx", "f04o"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "feiF1gdt", "KmGx"));

                StructuralVerify(tree);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                StructuralVerify(tree);

                Assert.Equal(5, tree.Count);
            }
        }

        [Fact]
        public void Structure_MultipleBranch_InternalExtent2()
        {
            InitializeStorage();

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "i", "8Jp3"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "4", "V6sl"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "j", "GX37"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "P", "f04o"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "8", "KmGx"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "3", "KmG3"));

                StructuralVerify(tree);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                StructuralVerify(tree);

                Assert.Equal(6, tree.Count);
            }
        }

        [Fact]
        public void Structure_MultipleBranch_OrderPreservation()
        {
            InitializeStorage();

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

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
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                StructuralVerify(tree);

                Assert.Equal(5, tree.Count);
            }
        }

        [Fact]
        public void Structure_MultipleBranch_OrderPreservation2()
        {
            InitializeStorage();

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "1Z", "8Jp3"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "fG", "V6sl"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "dW", "GX37"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "8I", "f04o"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "7H", "KmGx"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "73", "KmGx"));

                StructuralVerify(tree);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                StructuralVerify(tree);

                Assert.Equal(6, tree.Count);
            }
        }

        [Fact]
        public void Operations_Contains_SimpleCheck()
        {
            InitializeStorage();

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "Sr1", "8Jp3"));
                Assert.True(tree.Contains(new Slice(Encoding.UTF8.GetBytes("Sr1"))));
                Assert.False(tree.Contains(new Slice(Encoding.UTF8.GetBytes("MiL"))));

                StructuralVerify(tree);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                StructuralVerify(tree);

                Assert.Equal(1, tree.Count);
            }
        }

        [Fact]
        public void Structure_MultipleBranch_OrderPreservation3()
        {
            InitializeStorage();

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "6b", "8Jp3"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "ab", "V6sl"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "dG", "GX37"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "3s", "f04o"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "8u", "KmGx"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "cI", "KmGx"));

                StructuralVerify(tree);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                StructuralVerify(tree);

                Assert.Equal(6, tree.Count);
            }
        }

        [Fact]
        public void Structure_NodesTable_FailedTableVerify()
        {
            InitializeStorage();

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "R", "1q"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "F", "3n"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "O", "6e"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "E", "Fs"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "Lr", "LD"));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "L5", "MU"));

                StructuralVerify(tree);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                StructuralVerify(tree);

                Assert.Equal(6, tree.Count);
            }
        }

        [Fact]
        public void Addition_FailureToPass_QuickPath()
        {
            InitializeStorage();

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "lJCn3J", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "4wLolJ", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "FZt4Dp", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "8NSagc", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "9eI05C", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "C4gnS4", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "PRjxjs", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "3M7Oxy", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "boKWpa", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "FLnjoZ", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "AE1Jlq", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "mbHypw", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "FLnjhT", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "fvrTYR", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "2pOGiH", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "RpmKwf", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "1ulQmV", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "rn8YRe", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "wfnTE2", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "rqqjR5", string.Empty));

                StructuralVerify(tree);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                StructuralVerify(tree);

                Assert.Equal(20, tree.Count);
            }
        }

        [Fact]
        public void Addition_FailureToPass_QuickPath2()
        {
            InitializeStorage();

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "Sr1", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "MiL", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "P6V", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "Xfy", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "rBF", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "4H1", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "vdN", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "keF", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "9cf", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "hXe", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "SPf", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "Dq3", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "fa1", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "oyi", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "zme", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "bWf", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "bmv", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "ExG", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "xrM", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "wMG", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "prj", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "0FQ", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "SnV", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "LYr", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "9Gp", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "pu2", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "kiG", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "AGu", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "tNb", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "ZQ8", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "rtN", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "u9G", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "89g", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "OIt", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "KPO", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "DEr", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "aI7", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "TLo", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "Aol", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "yH1", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "y3W", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "Weo", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "dPG", string.Empty));
                Assert.NotEqual(-1, AddToPrefixTree(tree, docs, "iqv", string.Empty));              

                StructuralVerify(tree);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var tree = docs.GetPrefixTree(DocsSchema.Key.Name);

                StructuralVerify(tree);

                Assert.Equal(44, tree.Count);
            }
        }

        public static IEnumerable<object[]> TreeSize
        {
            get
            {
                // Or this could read from a file. :)
                return new[]
                {
                    new object[] { 102, 4, 4 },
                    new object[] { 100, 4, 8 },
                    new object[] { 101, 2, 128 },
                    new object[] { 100, 8, 5 },
                    new object[] { 100, 16, 168 }
                };
            }
        }
    }
}
