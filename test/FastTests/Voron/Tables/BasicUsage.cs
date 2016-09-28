// -----------------------------------------------------------------------
//  <copyright file="BasicUsage.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Text;
using Xunit;
using Voron;
using Voron.Data;
using Voron.Data.Tables;


namespace FastTests.Voron.Tables
{
    public unsafe class BasicUsage : TableStorageTest
    {

        [Fact]
        public  void CanInsertThenRead()
        {
            using (var tx = Env.WriteTransaction())
            {
                DocsSchema.Create(tx, "docs");

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");

                SetHelper(docs, "users/1",  "Users", 1L, "{'Name': 'Oren'}");

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");
                Slice key;
                Slice.From(tx.Allocator, "users/1", out key);
                var handle = docs.ReadByKey(key);

                int size;
                var read = handle.Read(3, out size);
                Assert.Equal("{'Name': 'Oren'}", Encoding.UTF8.GetString(read, size));
                tx.Commit();
            }
        }

        [Fact]
        public void CanInsertThenUpdateThenRead()
        {
            using (var tx = Env.WriteTransaction())
            {
                DocsSchema.Create(tx, "docs");

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");
                SetHelper(docs, "users/1", "Users", 1L, "{'Name': 'Oren'}");

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");
                SetHelper(docs, "users/1", "Users", 2L, "{'Name': 'Eini'}");

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");
                Slice key;
                Slice.From(tx.Allocator, "users/1", out key);
                var handle = docs.ReadByKey(key);

                int size;
                var read = handle.Read(3, out size);
                Assert.Equal("{'Name': 'Eini'}", Encoding.UTF8.GetString(read, size));

                tx.Commit();
            }
        }

        [Fact]
        public void CanInsertThenDelete()
        {
            using (var tx = Env.WriteTransaction())
            {
                DocsSchema.Create(tx, "docs");

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");
                SetHelper(docs, "users/1",  "Users", 1L, "{'Name': 'Oren'}");


                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");
                Slice key;
                Slice.From(tx.Allocator, "users/1", out key);
                docs.DeleteByKey(key);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");

                Slice key;
                Slice.From(tx.Allocator, "users/1", out key);
                Assert.Null(docs.ReadByKey(key));
            }
        }

        [Fact]
        public void HasCorrespondingRootObjectType()
        {
            using (var tx = Env.WriteTransaction())
            {
                DocsSchema.Create(tx, "docs");
                Slice key;
                Slice.From(tx.Allocator, "docs", out key);
                Assert.Equal(RootObjectType.Table, tx.GetRootObjectType(key));
                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                Slice key;
                Slice.From(tx.Allocator, "docs", out key);
                Assert.Equal(RootObjectType.Table, tx.GetRootObjectType(key));
            }
        }

    }
}