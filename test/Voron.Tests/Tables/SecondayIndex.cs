using System.Text;
using Bond;
using System;
using System.Linq;
using Voron.Data.Tables;
using Voron.Util.Conversion;
using Xunit;

namespace Voron.Tests.Tables
{
    public unsafe class SecondayIndex : TableStorageTest
    {

        [Fact]
        public void CanInsertThenReadBySecondary()
        {
            using (var tx = Env.WriteTransaction())
            {
                DocsSchema.Create(tx);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table(DocsSchema, tx);
                SetHelper(docs, "users/1", "Users", 1L, "{'Name': 'Oren'}");


                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table(DocsSchema, tx);

                var etag = new Slice(EndianBitConverter.Big.GetBytes(1L));
                var reader = docs.SeekTo("Etags", etag)
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
                DocsSchema.Create(tx);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table(DocsSchema, tx);
                SetHelper(docs, "users/1", "Users", 1L, "{'Name': 'Oren'}");

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table(DocsSchema, tx);

                docs.DeleteByKey("users/1");

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table(DocsSchema, tx);

                var reader = docs.SeekTo("Etags", new Slice(EndianBitConverter.Big.GetBytes(1)));
                Assert.Empty(reader);
            }
        }


        [Fact]
        public void CanInsertThenUpdateThenBySecondary()
        {
            using (var tx = Env.WriteTransaction())
            {
                DocsSchema.Create(tx);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table(DocsSchema, tx);
                SetHelper(docs, "users/1", "Users", 1L, "{'Name': 'Oren'}");


                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table(DocsSchema, tx);

                SetHelper(docs, "users/1", "Users", 2L, "{'Name': 'Eini'}");

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table(DocsSchema, tx);

                var etag = new Slice(EndianBitConverter.Big.GetBytes(1L));
                var reader = docs.SeekTo("Etags", etag)
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