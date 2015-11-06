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
                var docs = new Table<DocumentsFields>(_docsSchema, tx);

                var structure = new Structure<DocumentsFields>(_docsSchema.StructureSchema)
                    .Set(DocumentsFields.Etag, 1L)
                    .Set(DocumentsFields.Key, "users/1")
                    .Set(DocumentsFields.Data, "{'Name': 'Oren'}")
                    .Set(DocumentsFields.Collection, "Users");
                docs.Set(structure);

                structure = new Structure<DocumentsFields>(_docsSchema.StructureSchema)
                   .Set(DocumentsFields.Etag, 2L)
                   .Set(DocumentsFields.Key, "users/2")
                   .Set(DocumentsFields.Data, "{'Name': 'Eini'}")
                   .Set(DocumentsFields.Collection, "Users");
                docs.Set(structure);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table<DocumentsFields>(_docsSchema, tx);

                var seekResults = docs.SeekTo("By/Etag&Collection", "Users").GetEnumerator();
                Assert.True(seekResults.MoveNext());
                var reader = seekResults.Current;

                var valueReader = reader.Key.CreateReader();
                Assert.Equal("Users", valueReader.ReadString(5));
                Assert.Equal(1L, valueReader.ReadBigEndianInt64());
                var result = reader.Results.Single().ReadString(DocumentsFields.Data);
                Assert.Equal("{'Name': 'Oren'}", result);

                Assert.True(seekResults.MoveNext());
                reader = seekResults.Current;

                valueReader = reader.Key.CreateReader();
                Assert.Equal("Users", valueReader.ReadString(5));
                Assert.Equal(2L, valueReader.ReadBigEndianInt64());
                result = reader.Results.Single().ReadString(DocumentsFields.Data);
                Assert.Equal("{'Name': 'Eini'}", result);

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
                var docs = new Table<DocumentsFields>(_docsSchema, tx);

                docs.Set(new Structure<DocumentsFields>(_docsSchema.StructureSchema)
                    .Set(DocumentsFields.Etag, 1L)
                    .Set(DocumentsFields.Key, "users/1")
                    .Set(DocumentsFields.Data, "{'Name': 'Oren'}")
                    .Set(DocumentsFields.Collection, "Users")
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
                var docs = new Table<DocumentsFields>(_docsSchema, tx);

                docs.Set(new Structure<DocumentsFields>(_docsSchema.StructureSchema)
                    .Set(DocumentsFields.Etag, 1L)
                    .Set(DocumentsFields.Key, "users/1")
                    .Set(DocumentsFields.Data, "{'Name': 'Oren'}")
                    .Set(DocumentsFields.Collection, "Users")
                    );

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table<DocumentsFields>(_docsSchema, tx);

                var structure = new Structure<DocumentsFields>(_docsSchema.StructureSchema)
                    .Set(DocumentsFields.Etag, 2L)
                    .Set(DocumentsFields.Key, "users/1")
                    .Set(DocumentsFields.Data, "{'Name': 'Eini'}")
                    .Set(DocumentsFields.Collection, "Users");
                docs.Set(structure);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table<DocumentsFields>(_docsSchema, tx);

                var reader = docs.SeekTo("By/Etag&Collection", "Users")
                    .First();

                var valueReader = reader.Key.CreateReader();
                Assert.Equal("Users", valueReader.ReadString(5));
                Assert.Equal(2L, valueReader.ReadBigEndianInt64());

                var result = reader.Results.Single().ReadString(DocumentsFields.Data);
                Assert.Equal("{'Name': 'Eini'}", result);

                tx.Commit();
            }
        }

    }
}