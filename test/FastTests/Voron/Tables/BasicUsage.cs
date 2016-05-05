// -----------------------------------------------------------------------
//  <copyright file="BasicUsage.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Text;
using Xunit;
using Voron;
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
                var docs = new Table(DocsSchema, "docs", tx);

                SetHelper(docs, "users/1",  "Users", 1L, "{'Name': 'Oren'}");

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var handle = docs.ReadByKey(Slice.From(tx.Allocator, "users/1"));

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
                var docs = new Table(DocsSchema, "docs", tx);
                SetHelper(docs, "users/1", "Users", 1L, "{'Name': 'Oren'}");

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                SetHelper(docs, "users/1", "Users", 2L, "{'Name': 'Eini'}");

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);
                var handle = docs.ReadByKey(Slice.From(tx.Allocator, "users/1"));

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
                var docs = new Table(DocsSchema, "docs", tx);
                SetHelper(docs, "users/1",  "Users", 1L, "{'Name': 'Oren'}");


                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);

                docs.DeleteByKey(Slice.From(tx.Allocator, "users/1"));

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table(DocsSchema, "docs", tx);

                Assert.Null(docs.ReadByKey(Slice.From(tx.Allocator, "users/1")));
            }
        }

    }
}