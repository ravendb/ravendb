using System.Linq;
using System.Text;
using Voron;
using Voron.Data.Tables;
using Xunit;

namespace FastTests.Voron.Tables
{
    public unsafe class CompositeIndex : TableStorageTest
    {

        [Fact]
        public void CanInsertThenReadByComposite()
        {
            using (var tx = Env.WriteTransaction())
            {
                DocsSchema.Create(tx, "docs");

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");
                SetHelper(docs, "users/1","Users", 1L, "{'Name': 'Oren'}");
                SetHelper(docs, "users/2","Users", 2L,  "{'Name': 'Eini'}");

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");

                var seekResults = docs.SeekForwardFrom(DocsSchema.Indexes[EtagAndCollectionSlice], "Users").GetEnumerator();
                Assert.True(seekResults.MoveNext());
                var reader = seekResults.Current;

                var valueReader = reader.Key.CreateReader();
                Assert.Equal("Users", valueReader.ReadString(5));
                Assert.Equal(1L, valueReader.ReadBigEndianInt64());
                var handle = reader.Results.Single();
                int size;
                Assert.Equal("{'Name': 'Oren'}", Encoding.UTF8.GetString(handle.Read(3, out size), size));

                Assert.True(seekResults.MoveNext());
                reader = seekResults.Current;

                valueReader = reader.Key.CreateReader();
                Assert.Equal("Users", valueReader.ReadString(5));
                Assert.Equal(2L, valueReader.ReadBigEndianInt64());
                handle = reader.Results.Single();
                Assert.Equal("{'Name': 'Eini'}", Encoding.UTF8.GetString(handle.Read(3, out size), size));

                Assert.False(seekResults.MoveNext());
                tx.Commit();
            }
        }

        [Fact]
        public void CanInsertThenDeleteByComposite()
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

                docs.DeleteByKey(Slice.From(tx.Allocator, "users/1"));

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");

                var reader = docs.SeekForwardFrom(DocsSchema.Indexes[EtagAndCollectionSlice], "Users");
                Assert.Empty(reader);
            }
        }


        [Fact]
        public void CanInsertThenUpdateThenByComposite()
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

                var reader = docs.SeekForwardFrom(DocsSchema.Indexes[EtagAndCollectionSlice], "Users")
                                 .First();

                var valueReader = reader.Key.CreateReader();
                Assert.Equal("Users", valueReader.ReadString(5));
                Assert.Equal(2L, valueReader.ReadBigEndianInt64());

                var handle = reader.Results.Single();
                int size;
                Assert.Equal("{'Name': 'Eini'}", Encoding.UTF8.GetString(handle.Read(3, out size), size));

                tx.Commit();
            }
        }

    }
}