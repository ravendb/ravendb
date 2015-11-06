// -----------------------------------------------------------------------
//  <copyright file="BasicUsage.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

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
                var docs = new Table<DocumentsFields>(_docsSchema, tx);

                docs.Set(new Structure<DocumentsFields>(_docsSchema.StructureSchema)
                    .Set(DocumentsFields.Etag, 1L)
                    .Set(DocumentsFields.Key, "users/1")
                    .Set(DocumentsFields.Data, "{'Name': 'Oren'}")
                    );

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table<DocumentsFields>(_docsSchema, tx);

                var reader = docs.ReadByKey("users/1");
                var result = reader.ReadString(DocumentsFields.Data);
                Assert.Equal("{'Name': 'Oren'}", result);
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
                var docs = new Table<DocumentsFields>(_docsSchema, tx);

                docs.Set(new Structure<DocumentsFields>(_docsSchema.StructureSchema)
                    .Set(DocumentsFields.Etag, 1L)
                    .Set(DocumentsFields.Key, "users/1")
                    .Set(DocumentsFields.Data, "{'Name': 'Oren'}")
                    );

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table<DocumentsFields>(_docsSchema, tx);

                docs.Set(new Structure<DocumentsFields>(_docsSchema.StructureSchema)
                    .Set(DocumentsFields.Etag, 1L)
                    .Set(DocumentsFields.Key, "users/1")
                    .Set(DocumentsFields.Data, "{'Name': 'Eini'}")
                    );

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table<DocumentsFields>(_docsSchema, tx);

                var reader = docs.ReadByKey("users/1");
                var result = reader.ReadString(DocumentsFields.Data);
                Assert.Equal("{'Name': 'Eini'}", result);
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
                var docs = new Table<DocumentsFields>(_docsSchema, tx);

                docs.Set(new Structure<DocumentsFields>(_docsSchema.StructureSchema)
                    .Set(DocumentsFields.Etag, 1L)
                    .Set(DocumentsFields.Key, "users/1")
                    .Set(DocumentsFields.Data, "{'Name': 'Oren'}")
                    );

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table<DocumentsFields>(_docsSchema, tx);

                docs.DeleteByKey("users/1");

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table<DocumentsFields>(_docsSchema, tx);

                var reader = docs.ReadByKey("users/1");
                Assert.Null(reader);
            }
        }

    }
}