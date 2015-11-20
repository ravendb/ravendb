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
                // var docs = new Table<DocumentsFields>(_docsSchema, tx);
                var docs = new Table<Documents>(_docsSchema, tx);

                //var structure = new Structure<DocumentsFields>(_docsSchema.StructureSchema)
                //    .Set(DocumentsFields.Etag, 1L)
                //    .Set(DocumentsFields.Key, "users/1")
                //    .Set(DocumentsFields.Data, "{'Name': 'Oren'}")
                //    .Set(DocumentsFields.Collection, "Users");
                //docs.Set(structure);

                var doc = new Documents { Etag = 1L, Key = "users/1", Data = "{'Name': 'Oren'}", Collection = "Users" };
                docs.Set(doc);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                // var docs = new Table<DocumentsFields>(_docsSchema, tx);
                var docs = new Table<Documents>(_docsSchema, tx);

                var etag = new Slice(EndianBitConverter.Big.GetBytes(1L));
                var reader = docs.SeekTo("By/Etag", etag)
                                 .First();

                Assert.Equal(1L, reader.Key.CreateReader().ReadBigEndianInt64());
                var result = reader.Results.Single();
                Assert.Equal("{'Name': 'Oren'}", result.Data);

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
                // var docs = new Table<DocumentsFields>(_docsSchema, tx);
                var docs = new Table<Documents>(_docsSchema, tx);

                //docs.Set(new Structure<DocumentsFields>(_docsSchema.StructureSchema)
                //    .Set(DocumentsFields.Etag, 1L)
                //    .Set(DocumentsFields.Key, "users/1")
                //    .Set(DocumentsFields.Data, "{'Name': 'Oren'}")
                //    .Set(DocumentsFields.Collection, "Users")
                //    );

                var doc = new Documents { Etag = 1L, Key = "users/1", Data = "{'Name': 'Oren'}", Collection = "Users" };
                docs.Set(doc);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                // var docs = new Table<DocumentsFields>(_docsSchema, tx);
                var docs = new Table<Documents>(_docsSchema, tx);

                docs.DeleteByKey("users/1");

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                // var docs = new Table<DocumentsFields>(_docsSchema, tx);
                var docs = new Table<Documents>(_docsSchema, tx);

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
                // var docs = new Table<DocumentsFields>(_docsSchema, tx);
                var docs = new Table<Documents>(_docsSchema, tx);

                //docs.Set(new Structure<DocumentsFields>(_docsSchema.StructureSchema)
                //    .Set(DocumentsFields.Etag, 1L)
                //    .Set(DocumentsFields.Key, "users/1")
                //    .Set(DocumentsFields.Data, "{'Name': 'Oren'}")
                //    .Set(DocumentsFields.Collection, "Users")
                //    );

                var doc = new Documents { Etag = 1L, Key = "users/1", Data = "{'Name': 'Oren'}", Collection = "Users" };
                docs.Set(doc);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                // var docs = new Table<DocumentsFields>(_docsSchema, tx);
                var docs = new Table<Documents>(_docsSchema, tx);

                //var structure = new Structure<DocumentsFields>(_docsSchema.StructureSchema)
                //    .Set(DocumentsFields.Etag, 2L)
                //    .Set(DocumentsFields.Key, "users/1")
                //    .Set(DocumentsFields.Collection, "Users")
                //    .Set(DocumentsFields.Data, "{'Name': 'Eini'}");
                //docs.Set(structure);

                var doc = new Documents { Etag = 2L, Key = "users/1", Data = "{'Name': 'Eini'}", Collection = "Users" };
                docs.Set(doc);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                // var docs = new Table<DocumentsFields>(_docsSchema, tx);
                var docs = new Table<Documents>(_docsSchema, tx);

                var etag = new Slice(EndianBitConverter.Big.GetBytes(1L));
                var reader = docs.SeekTo("By/Etag", etag)
                                 .First();

                Assert.Equal(2L, reader.Key.CreateReader().ReadBigEndianInt64());
                var result = reader.Results.Single();
                Assert.Equal("{'Name': 'Eini'}", result.Data);

                tx.Commit();
            }
        }

    }
}