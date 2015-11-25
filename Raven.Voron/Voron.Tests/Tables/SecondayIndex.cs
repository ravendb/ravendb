using Bond;
using System;
using System.Linq;
using Voron.Data.Tables;
using Voron.Util.Conversion;
using Xunit;

namespace Voron.Tests.Tables
{
    public class SecondayIndex : TableStorageTest
    {

        [Fact]
        public void CanInsertThenReadBySecondary()
        {
            using (var tx = Env.WriteTransaction())
            {
                _docsSchema.Create(tx);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table<Documents, DocumentData>(_docsSchema, tx);

                var doc = new Documents { Etag = 1L, Key = "users/1", Collection = "Users" };
                docs.Set(doc, new DocumentData { Data = "{'Name': 'Oren'}" });

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table<Documents, DocumentData>(_docsSchema, tx);

                var etag = new Slice(EndianBitConverter.Big.GetBytes(1L));
                var reader = docs.SeekTo("By/Etag", etag)
                                 .First();

                Assert.Equal(1L, reader.Key.CreateReader().ReadBigEndianInt64());
                var handle = reader.Results.Single();
                Assert.Equal("{'Name': 'Oren'}", handle.Value.Data);

                tx.Commit();
            }
        }

        [Fact]
        public void CanInsertThenDeleteBySecondary()
        {
            using (var tx = Env.WriteTransaction())
            {
                _docsSchema.Create(tx);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table<Documents, DocumentData>(_docsSchema, tx);

                var doc = new Documents { Etag = 1L, Key = "users/1", Collection = "Users" };
                docs.Set(doc, new DocumentData { Data = "{'Name': 'Oren'}" });

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table<Documents, DocumentData>(_docsSchema, tx);

                docs.DeleteByKey("users/1");

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table<Documents, DocumentData>(_docsSchema, tx);

                var reader = docs.SeekTo("By/Etag", new Slice(EndianBitConverter.Big.GetBytes(1)));
                Assert.Empty(reader);
            }
        }


        [Fact]
        public void CanInsertThenUpdateThenBySecondary()
        {
            using (var tx = Env.WriteTransaction())
            {
                _docsSchema.Create(tx);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table<Documents, DocumentData>(_docsSchema, tx);

                var doc = new Documents { Etag = 1L, Key = "users/1", Collection = "Users" };
                docs.Set(doc, new DocumentData { Data = "{'Name': 'Oren'}" });

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table<Documents, DocumentData>(_docsSchema, tx);

                var doc = new Documents { Etag = 2L, Key = "users/1", Collection = "Users" };
                docs.Set(doc, new DocumentData { Data = "{'Name': 'Eini'}" });

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table<Documents, DocumentData>(_docsSchema, tx);

                var etag = new Slice(EndianBitConverter.Big.GetBytes(1L));
                var reader = docs.SeekTo("By/Etag", etag)
                                 .First();

                Assert.Equal(2L, reader.Key.CreateReader().ReadBigEndianInt64());

                var handle = reader.Results.Single();
                Assert.Equal("{'Name': 'Eini'}", handle.Value.Data);

                tx.Commit();
            }
        }

    }
}