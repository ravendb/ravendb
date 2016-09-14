using System.Linq;
using System.Text;
using Voron;
using Voron.Data.Tables;
using Voron.Util.Conversion;
using Xunit;

namespace FastTests.Voron.Tables
{
    public unsafe class SecondayIndex : TableStorageTest
    {

        [Fact]
        public void CanInsertThenReadBySecondary()
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

            using (var tx = Env.ReadTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");

                var etag = Slice.From(Allocator, EndianBitConverter.Big.GetBytes(1L));
                var reader = docs.SeekForwardFrom(DocsSchema.Indexes[Etags], etag)
                                 .First();

                Assert.Equal(1L, reader.Key.CreateReader().ReadBigEndianInt64());
                var handle = reader.Results.Single();
                int size;
                Assert.Equal("{'Name': 'Oren'}", Encoding.UTF8.GetString(handle.Read(3, out size), size));


                tx.Commit();
            }
        }

        [Fact]
        public void CanInsertThenDeleteBySecondary()
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

                var reader = docs.SeekForwardFrom(DocsSchema.Indexes[Etags], Slice.From(Allocator, EndianBitConverter.Big.GetBytes(1)));
                Assert.Empty(reader);
            }
        }


        [Fact]
        public void CanInsertThenUpdateThenBySecondary()
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

                var etag = Slice.From(Allocator, EndianBitConverter.Big.GetBytes(1L));
                var reader = docs.SeekForwardFrom(DocsSchema.Indexes[Etags], etag)
                                 .First();

                Assert.Equal(2L, reader.Key.CreateReader().ReadBigEndianInt64());

                var handle = reader.Results.Single();
                int size;
                Assert.Equal("{'Name': 'Eini'}", Encoding.UTF8.GetString(handle.Read(3, out size), size));


                tx.Commit();
            }
        }

    }
}