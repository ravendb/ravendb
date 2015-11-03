// -----------------------------------------------------------------------
//  <copyright file="BasicUsage.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Voron.Data.RawData;
using Voron.Data.Tables;
using Voron.Impl;
using Voron.Impl.FileHeaders;
using Voron.Impl.Paging;
using Voron.Util.Conversion;
using Xunit;

namespace Voron.Tests.Tables
{
    public class BasicUsage : StorageTest
    {
        private TableSchema<DocumentsFields> _schema;

        [Fact]
        public void CanInsertThenRead()
        {
            using (var tx = Env.WriteTransaction())
            {
                _schema.Create(tx);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table<DocumentsFields>(_schema, tx);

                docs.Set(new Structure<DocumentsFields>(_schema.StructureSchema)
                    .Set(DocumentsFields.Etag, 1L)
                    .Set(DocumentsFields.Key, "users/1")
                    .Set(DocumentsFields.Data, "{'Name': 'Oren'}")
                    );

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table<DocumentsFields>(_schema, tx);

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
                _schema.Create(tx);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table<DocumentsFields>(_schema, tx);

                docs.Set(new Structure<DocumentsFields>(_schema.StructureSchema)
                    .Set(DocumentsFields.Etag, 1L)
                    .Set(DocumentsFields.Key, "users/1")
                    .Set(DocumentsFields.Data, "{'Name': 'Oren'}")
                    );

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table<DocumentsFields>(_schema, tx);

                docs.Set(new Structure<DocumentsFields>(_schema.StructureSchema)
                    .Set(DocumentsFields.Etag, 1L)
                    .Set(DocumentsFields.Key, "users/1")
                    .Set(DocumentsFields.Data, "{'Name': 'Eini'}")
                    );

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table<DocumentsFields>(_schema, tx);

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
                _schema.Create(tx);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table<DocumentsFields>(_schema, tx);

                docs.Set(new Structure<DocumentsFields>(_schema.StructureSchema)
                    .Set(DocumentsFields.Etag, 1L)
                    .Set(DocumentsFields.Key, "users/1")
                    .Set(DocumentsFields.Data, "{'Name': 'Oren'}")
                    );

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table<DocumentsFields>(_schema, tx);

                docs.DeleteByKey("users/1");

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table<DocumentsFields>(_schema, tx);

                var reader = docs.ReadByKey("users/1");
                Assert.Null(reader);
            }
        }

        public enum DocumentsFields
        {
            Etag,
            Key,
            Data
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            base.Configure(options);

            _schema = new TableSchema<DocumentsFields>("docs")
                
                .DefineField<long>(DocumentsFields.Etag)
                .DefineField<string>(DocumentsFields.Key)
                .DefineField<string>(DocumentsFields.Data)

                .DefineIndex("By/Etag", DocumentsFields.Etag)
                .DefineKey(DocumentsFields.Key);
        }
    }

}