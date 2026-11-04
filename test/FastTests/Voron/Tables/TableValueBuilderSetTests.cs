using System;
using System.Text;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Server;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Tables;
using Xunit;

namespace FastTests.Voron.Tables
{
    public unsafe class TableValueBuilderSetTests(ITestOutputHelper output) : TableStorageTest(output)
    {
        private const string Data = "{'Name': 'Oren'}";

        private static readonly Slice CompressionSlice;

        static TableValueBuilderSetTests()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "Compression", ByteStringType.Immutable, out CompressionSlice);
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void AllocateFromSourceCopiesEveryColumn()
        {
            CreateDocsTableWith("users/1", "Users", 1L, Data);

            using (var tx = Env.WriteTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");
                using (Slice.From(tx.Allocator, "users/1", out Slice sourceKey))
                using (Slice.From(tx.Allocator, "users/2", out Slice newKey))
                {
                    Assert.True(docs.ReadByKey(sourceKey, out TableValueReader source));

                    using (docs.Allocate(source, out TableValueBuilder builder))
                    {
                        Assert.Equal(source.Count, builder.Count);
                        for (int i = 0; i < source.Count; i++)
                        {
                            source.Read(i, out int size);
                            Assert.Equal(size, builder.SizeOf(i));
                        }

                        builder.Set(0, newKey);
                        docs.Insert(builder);
                    }
                }

                tx.Commit();
            }

            AssertRow("users/1", "Users", 1L, Data, null);
            AssertRow("users/2", "Users", 1L, Data, null);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void SetReplacesMiddleColumnsWithValuesOfDifferentSize()
        {
            CreateDocsTableWith("users/1", "Users", 1L, Data);

            using (var tx = Env.WriteTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");
                using (Slice.From(tx.Allocator, "users/1", out Slice sourceKey))
                using (Slice.From(tx.Allocator, "users/2", out Slice newKey))
                using (Slice.From(tx.Allocator, "U", out Slice shorterCollection))
                using (Slice.From(tx.Allocator, "{'Name': 'Oren Eini', 'Age': 42}", out Slice longerData))
                {
                    Assert.True(docs.ReadByKey(sourceKey, out TableValueReader source));

                    using (docs.Allocate(source, out TableValueBuilder builder))
                    {
                        builder.Set(0, newKey);
                        builder.Set(1, shorterCollection);
                        builder.Set(2, Bits.SwapBytes(7L));
                        builder.Set(3, longerData);
                        Assert.Equal(4, builder.Count);

                        docs.Insert(builder);
                    }
                }

                tx.Commit();
            }

            AssertRow("users/2", "U", 7L, "{'Name': 'Oren Eini', 'Age': 42}", null);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void SetAtCountAppendsColumn()
        {
            CreateDocsTableWith("users/1", "Users", 1L, Data);

            using (var tx = Env.WriteTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");
                using (Slice.From(tx.Allocator, "users/1", out Slice sourceKey))
                using (Slice.From(tx.Allocator, "users/2", out Slice newKey))
                using (Slice.From(tx.Allocator, "appended", out Slice appended))
                {
                    Assert.True(docs.ReadByKey(sourceKey, out TableValueReader source));

                    using (docs.Allocate(source, out TableValueBuilder builder))
                    {
                        builder.Set(0, newKey);
                        builder.Set(builder.Count, appended);
                        Assert.Equal(5, builder.Count);

                        docs.Insert(builder);
                    }
                }

                tx.Commit();
            }

            AssertRow("users/2", "Users", 1L, Data, "appended");
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void SetOnAppendedColumnReplacesIt()
        {
            CreateDocsTableWith("users/1", "Users", 1L, Data);

            using (var tx = Env.WriteTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");
                using (Slice.From(tx.Allocator, "users/1", out Slice sourceKey))
                using (Slice.From(tx.Allocator, "users/2", out Slice newKey))
                using (Slice.From(tx.Allocator, "first", out Slice first))
                using (Slice.From(tx.Allocator, "second, and longer", out Slice second))
                {
                    Assert.True(docs.ReadByKey(sourceKey, out TableValueReader source));

                    using (docs.Allocate(source, out TableValueBuilder builder))
                    {
                        builder.Set(0, newKey);
                        builder.Set(4, first);
                        builder.Set(4, second);
                        Assert.Equal(5, builder.Count);

                        docs.Insert(builder);
                    }
                }

                tx.Commit();
            }

            AssertRow("users/2", "Users", 1L, Data, "second, and longer");
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void SetPastCountThrows()
        {
            CreateDocsTableWith("users/1", "Users", 1L, Data);

            using (var tx = Env.WriteTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");
                using (Slice.From(tx.Allocator, "users/1", out Slice sourceKey))
                using (Slice.From(tx.Allocator, "past", out Slice past))
                {
                    Assert.True(docs.ReadByKey(sourceKey, out TableValueReader source));

                    using (docs.Allocate(source, out TableValueBuilder builder))
                    {
                        Assert.NotNull(TrySet(builder, builder.Count + 1, past));
                        Assert.NotNull(TrySet(builder, -1, past));
                        Assert.Equal(4, builder.Count);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void SetThatGrowsPastByteOffsetsStillReadsBack()
        {
            var largeData = new string('x', 300);
            CreateDocsTableWith("users/1", "Users", 1L, Data);

            using (var tx = Env.WriteTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");
                using (Slice.From(tx.Allocator, "users/1", out Slice sourceKey))
                using (Slice.From(tx.Allocator, "users/2", out Slice newKey))
                using (Slice.From(tx.Allocator, largeData, out Slice large))
                {
                    Assert.True(docs.ReadByKey(sourceKey, out TableValueReader source));

                    using (docs.Allocate(source, out TableValueBuilder builder))
                    {
                        Assert.Equal(1, builder.ElementSize);

                        builder.Set(0, newKey);
                        builder.Set(3, large);
                        Assert.Equal(2, builder.ElementSize);

                        docs.Insert(builder);
                    }
                }

                tx.Commit();
            }

            AssertRow("users/2", "Users", 1L, largeData, null);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void SetThatShrinksBelowByteOffsetsStillReadsBack()
        {
            CreateDocsTableWith("users/1", "Users", 1L, new string('x', 300));

            using (var tx = Env.WriteTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");
                using (Slice.From(tx.Allocator, "users/1", out Slice sourceKey))
                using (Slice.From(tx.Allocator, "users/2", out Slice newKey))
                using (Slice.From(tx.Allocator, Data, out Slice small))
                {
                    Assert.True(docs.ReadByKey(sourceKey, out TableValueReader source));

                    using (docs.Allocate(source, out TableValueBuilder builder))
                    {
                        builder.Set(0, newKey);
                        builder.Set(3, small);

                        docs.Insert(builder);
                    }
                }

                tx.Commit();
            }

            AssertRow("users/2", "Users", 1L, Data, null);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void SetThatShrinksBelowByteOffsetsAfterSizeWasReadStillReadsBack()
        {
            CreateDocsTableWith("users/1", "Users", 1L, new string('x', 300));

            using (var tx = Env.WriteTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");
                using (Slice.From(tx.Allocator, "users/1", out Slice sourceKey))
                using (Slice.From(tx.Allocator, "users/2", out Slice newKey))
                using (Slice.From(tx.Allocator, Data, out Slice small))
                {
                    Assert.True(docs.ReadByKey(sourceKey, out TableValueReader source));

                    using (docs.Allocate(source, out TableValueBuilder builder))
                    {
                        // reading the size while the row is large settles the offset width at 2 bytes
                        Assert.Equal(source.Size, builder.Size);

                        builder.Set(0, newKey);
                        builder.Set(3, small);

                        docs.Insert(builder);
                    }
                }

                tx.Commit();
            }

            AssertRow("users/2", "Users", 1L, Data, null);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void SourceDeletedByIdBeforeInsert()
        {
            CreateDocsTableWith("users/1", "Users", 1L, Data);

            using (var tx = Env.WriteTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");
                using (Slice.From(tx.Allocator, "users/1", out Slice sourceKey))
                using (Slice.From(tx.Allocator, "users/2", out Slice newKey))
                {
                    Assert.True(docs.ReadByKey(sourceKey, out TableValueReader source));

                    using (docs.Allocate(source, out TableValueBuilder builder))
                    {
                        builder.Set(0, newKey);
                        docs.Delete(source.Id);
                        docs.Insert(builder);
                    }
                }

                tx.Commit();
            }

            AssertNoRow("users/1");
            AssertRow("users/2", "Users", 1L, Data, null);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void SourceDeletedByKeyBeforeInsertUnderTheSameKey()
        {
            CreateDocsTableWith("users/1", "Users", 1L, Data);

            using (var tx = Env.WriteTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");
                using (Slice.From(tx.Allocator, "users/1", out Slice sourceKey))
                using (Slice.From(tx.Allocator, "replaced", out Slice appended))
                {
                    Assert.True(docs.ReadByKey(sourceKey, out TableValueReader source));

                    using (docs.Allocate(source, out TableValueBuilder builder))
                    {
                        builder.Set(2, Bits.SwapBytes(2L));
                        builder.Set(builder.Count, appended);
                        Assert.True(docs.DeleteByKey(sourceKey));
                        docs.Insert(builder);
                    }
                }

                tx.Commit();
            }

            AssertRow("users/1", "Users", 2L, Data, "replaced");
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void SourceDeletedFromCompressedTableBeforeInsert()
        {
            var data = CompressibleData();
            TableSchema schema;

            using (var tx = Env.WriteTransaction())
            {
                var fixedSizedIndex = new TableSchema.FixedSizeKeyIndexDef
                {
                    Name = CompressionSlice,
                    IsGlobal = true,
                    StartIndex = 1,
                };

                schema = new TableSchema()
                    .CompressValues(fixedSizedIndex, true)
                    .DefineFixedSizeIndex(fixedSizedIndex)
                    .DefineKey(new TableSchema.IndexDef
                    {
                        StartIndex = 0,
                        Count = 1,
                    });

                schema.Create(tx, "Items", 16);
                var items = tx.OpenTable(schema, "Items");

                using (items.Allocate(out TableValueBuilder builder))
                using (Slice.From(tx.Allocator, "items/1", out Slice key))
                using (Slice.From(tx.Allocator, data, out Slice value))
                {
                    builder.Add(key);
                    builder.Add(Bits.SwapBytes(1L));
                    builder.Add(value);
                    long id = items.Insert(builder);
                    Assert.True(items.GetInfoFor(id).IsCompressed);
                }

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var items = tx.OpenTable(schema, "Items");
                using (Slice.From(tx.Allocator, "items/1", out Slice sourceKey))
                using (Slice.From(tx.Allocator, "items/2", out Slice newKey))
                {
                    Assert.True(items.ReadByKey(sourceKey, out TableValueReader source));

                    using (items.Allocate(source, out TableValueBuilder builder))
                    {
                        builder.Set(0, newKey);
                        builder.Set(1, Bits.SwapBytes(2L));

                        // deleting a compressed row releases its decompressed buffer; reuse that memory
                        items.Delete(source.Id);
                        using (tx.Allocator.Allocate(source.Size, out ByteString filler))
                        {
                            Memory.Set(filler.Ptr, 0, source.Size);
                            long id = items.Insert(builder);
                            Assert.True(items.GetInfoFor(id).IsCompressed);
                        }
                    }
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var items = tx.OpenTable(schema, "Items");
                using (Slice.From(tx.Allocator, "items/1", out Slice sourceKey))
                using (Slice.From(tx.Allocator, "items/2", out Slice newKey))
                {
                    Assert.False(items.ReadByKey(sourceKey, out _));
                    Assert.True(items.ReadByKey(newKey, out TableValueReader reader));

                    Assert.Equal(3, reader.Count);
                    Assert.Equal("items/2", reader.ReadString(0));
                    Assert.Equal(2L, Bits.SwapBytes(reader.ReadLong(1)));
                    Assert.Equal(data, reader.ReadString(2));
                }
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void PlainAllocateWorksAfterSourceScopeIsDisposed()
        {
            CreateDocsTableWith("users/1", "Users", 1L, new string('x', 300));

            using (var tx = Env.WriteTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");
                using (Slice.From(tx.Allocator, "users/1", out Slice sourceKey))
                using (Slice.From(tx.Allocator, "users/2", out Slice copyKey))
                using (Slice.From(tx.Allocator, "users/3", out Slice plainKey))
                using (Slice.From(tx.Allocator, "Users", out Slice collection))
                using (Slice.From(tx.Allocator, Data, out Slice data))
                {
                    Assert.True(docs.ReadByKey(sourceKey, out TableValueReader source));

                    using (docs.Allocate(source, out TableValueBuilder builder))
                    {
                        builder.Set(0, copyKey);
                        builder.Set(builder.Count, collection);
                        Assert.Equal(2, builder.ElementSize);

                        docs.Insert(builder);
                    }

                    using (docs.Allocate(out TableValueBuilder builder))
                    {
                        Assert.Equal(0, builder.Count);
                        Assert.Equal(1, builder.ElementSize);

                        builder.Add(plainKey);
                        builder.Add(collection);
                        builder.Add(Bits.SwapBytes(3L));
                        builder.Add(data);

                        docs.Insert(builder);
                    }
                }

                tx.Commit();
            }

            AssertRow("users/2", "Users", 1L, new string('x', 300), "Users");
            AssertRow("users/3", "Users", 3L, Data, null);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void AllocateFromSourceWithNoColumns()
        {
            using (var tx = Env.WriteTransaction())
            {
                DocsSchema.Create(tx, "docs", 16);
                var docs = tx.OpenTable(DocsSchema, "docs");

                // a value with no columns is just its column count
                fixed (byte* empty = new byte[] { 0 })
                using (Slice.From(tx.Allocator, "users/1", out Slice key))
                {
                    var source = new TableValueReader(empty, 1);

                    using (docs.Allocate(source, out TableValueBuilder builder))
                    {
                        Assert.Equal(0, builder.Count);

                        builder.Set(0, key);
                        Assert.Equal(1, builder.Count);
                        Assert.Equal(key.Size, builder.SizeOf(0));
                    }
                }
            }
        }

        private void CreateDocsTableWith(string key, string collection, long etag, string data)
        {
            using (var tx = Env.WriteTransaction())
            {
                DocsSchema.Create(tx, "docs", 16);
                var docs = tx.OpenTable(DocsSchema, "docs");
                SetHelper(docs, key, collection, etag, data);

                tx.Commit();
            }
        }

        private void AssertRow(string key, string collection, long etag, string data, string appended)
        {
            using (var tx = Env.ReadTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");
                using (Slice.From(tx.Allocator, key, out Slice keySlice))
                {
                    Assert.True(docs.ReadByKey(keySlice, out TableValueReader reader));

                    Assert.Equal(appended == null ? 4 : 5, reader.Count);
                    Assert.Equal(key, reader.ReadString(0));
                    Assert.Equal(collection, reader.ReadString(1));
                    Assert.Equal(etag, Bits.SwapBytes(reader.ReadLong(2)));
                    Assert.Equal(data, reader.ReadString(3));
                    if (appended != null)
                        Assert.Equal(appended, reader.ReadString(4));
                }
            }
        }

        private void AssertNoRow(string key)
        {
            using (var tx = Env.ReadTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");
                using (Slice.From(tx.Allocator, key, out Slice keySlice))
                {
                    Assert.False(docs.ReadByKey(keySlice, out _));
                }
            }
        }

        private static ArgumentOutOfRangeException TrySet(TableValueBuilder builder, int index, Slice value)
        {
            try
            {
                builder.Set(index, value);
                return null;
            }
            catch (ArgumentOutOfRangeException e)
            {
                return e;
            }
        }

        private static string CompressibleData()
        {
            var random = new Random(49941);
            var data = new StringBuilder();
            for (int i = 0; i < 100; i++)
            {
                var bytes = new byte[16];
                random.NextBytes(bytes);
                if (i > 0)
                    data.Append(", ");
                data.Append(new Guid(bytes));
            }

            return data.ToString();
        }
    }
}
