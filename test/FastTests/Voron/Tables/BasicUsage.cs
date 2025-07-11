using System.Text;
using Tests.Infrastructure;
using Voron;
using Voron.Data;
using Voron.Data.Tables;
using Xunit;
using Xunit.Abstractions;


namespace FastTests.Voron.Tables
{
    public unsafe class BasicUsage(ITestOutputHelper output) : TableStorageTest(output)
    {
        [RavenFact(RavenTestCategory.Voron)]
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

        [RavenFact(RavenTestCategory.Voron)]
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

        [RavenFact(RavenTestCategory.Voron)]
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

        [RavenFact(RavenTestCategory.Voron)]
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
