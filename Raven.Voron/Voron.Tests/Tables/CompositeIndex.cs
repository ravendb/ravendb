using Bond;
using System;
using System.Collections.Generic;
using System.Linq;
using Voron.Data.Tables;
using Voron.Util.Conversion;
using Xunit;

namespace Voron.Tests.Tables
{
    public class CompositeIndex : TableStorageTest
    {

        [Fact]
        public void CanInsertThenReadByComposite()
        {
            using (var tx = Env.WriteTransaction())
            {
                _docsSchema.Create(tx);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table<Documents>(_docsSchema, tx);

                var doc = new Documents { Etag = 1L, Key = "users/1", Data = "{'Name': 'Oren'}", Collection = "Users" };
                docs.Set(doc);

                doc = new Documents { Etag = 2L, Key = "users/2", Data = "{'Name': 'Eini'}", Collection = "Users" };
                docs.Set(doc);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table<Documents>(_docsSchema, tx);

                var seekResults = docs.SeekTo("By/Etag&Collection", "Users").GetEnumerator();
                Assert.True(seekResults.MoveNext());
                var reader = seekResults.Current;

                var valueReader = reader.Key.CreateReader();
                Assert.Equal("Users", valueReader.ReadString(5));
                Assert.Equal(1L, valueReader.ReadBigEndianInt64());
                var result = reader.Results.Single();
                Assert.Equal("{'Name': 'Oren'}", result.Data);

                Assert.True(seekResults.MoveNext());
                reader = seekResults.Current;

                valueReader = reader.Key.CreateReader();
                Assert.Equal("Users", valueReader.ReadString(5));
                Assert.Equal(2L, valueReader.ReadBigEndianInt64());
                result = reader.Results.Single();
                Assert.Equal("{'Name': 'Eini'}", result.Data);

                Assert.False(seekResults.MoveNext());
                tx.Commit();
            }
        }

        [Fact]
        public void CanInsertThenDeleteByComposite()
        {
            using (var tx = Env.WriteTransaction())
            {
                _docsSchema.Create(tx);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table<Documents>(_docsSchema, tx);

                var doc = new Documents { Etag = 1L, Key = "users/1", Data = "{'Name': 'Oren'}", Collection = "Users" };
                docs.Set(doc);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table<Documents>(_docsSchema, tx);

                docs.DeleteByKey("users/1");

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table<Documents>(_docsSchema, tx);

                var reader = docs.SeekTo("By/Etag&Collection", "Users");
                Assert.Empty(reader);
            }
        }


        [Fact]
        public void CanInsertThenUpdateThenByComposite()
        {
            using (var tx = Env.WriteTransaction())
            {
                _docsSchema.Create(tx);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table<Documents>(_docsSchema, tx);

                var doc = new Documents { Etag = 1L, Key = "users/1", Data = "{'Name': 'Oren'}", Collection = "Users" };
                docs.Set(doc);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table<Documents>(_docsSchema, tx);

                var doc = new Documents { Etag = 2L, Key = "users/1", Data = "{'Name': 'Eini'}", Collection = "Users" };
                docs.Set(doc);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table<Documents>(_docsSchema, tx);

                var reader = docs.SeekTo("By/Etag&Collection", "Users")
                                 .First();

                var valueReader = reader.Key.CreateReader();
                Assert.Equal("Users", valueReader.ReadString(5));
                Assert.Equal(2L, valueReader.ReadBigEndianInt64());

                var result = reader.Results.Single();
                Assert.Equal("{'Name': 'Eini'}", result.Data);

                tx.Commit();
            }
        }

    }
}