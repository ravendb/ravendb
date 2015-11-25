// -----------------------------------------------------------------------
//  <copyright file="BasicUsage.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Bond;
using Voron.Data.Tables;
using Xunit;

namespace Voron.Tests.Tables
{
    public class BasicUsage : TableStorageTest
    {

        [Fact]
        public void CanInsertThenRead()
        {
            using (var tx = Env.WriteTransaction())
            {
                _docsSchema.Create(tx);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                // var docs = new Table<DocumentsFields>(_docsSchema, tx);
                var docs = new Table<Documents>(_docsSchema, tx);

                var doc = new Documents { Etag = 1L, Key = "users/1", Data = "{'Name': 'Oren'}", Collection = "Users" };
                docs.Set(doc);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table<Documents>(_docsSchema, tx);
                var doc = docs.ReadByKey("users/1");

                Assert.Equal("{'Name': 'Oren'}", doc.Data);
                tx.Commit();
            }
        }

        [Fact]
        public void CanInsertThenUpdateThenRead()
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

                var doc = new Documents { Etag = 1L, Key = "users/1", Data = "{'Name': 'Eini'}", Collection = "Users" };
                docs.Set(doc);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table<Documents>(_docsSchema, tx);
                var doc = docs.ReadByKey("users/1");

                Assert.Equal("{'Name': 'Eini'}", doc.Data);
                tx.Commit();
            }
        }

        [Fact]
        public void CanInsertThenDelete()
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

                var doc = docs.ReadByKey("users/1");
                Assert.Null(doc);
            }
        }

    }
}