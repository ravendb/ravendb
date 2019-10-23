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
using Xunit.Abstractions;


namespace FastTests.Voron.Tables
{
    public unsafe class BasicUsage : TableStorageTest
    {
        public BasicUsage(ITestOutputHelper output) : base(output)
        {
        }


        [Fact]
        public  void CanInsertThenRead()
        {
            using (var tx = Env.WriteTransaction())
            {
                DocsSchema.Create(tx, "docs", 16);

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
                TableValueReader handle;
                Assert.True(docs.ReadByKey(key,out handle));

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
                DocsSchema.Create(tx, "docs",16);

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
                TableValueReader handle;
                Assert.True(docs.ReadByKey(key, out handle));

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
                DocsSchema.Create(tx, "docs", 16);

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
                TableValueReader reader;
                Assert.False(docs.ReadByKey(key,out reader));
            }
        }

        [Fact]
        public void HasCorrespondingRootObjectType()
        {
            using (var tx = Env.WriteTransaction())
            {
                DocsSchema.Create(tx, "docs", 16);
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
