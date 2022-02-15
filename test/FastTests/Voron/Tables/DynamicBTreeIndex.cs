using System.Text;
using Sparrow;
using Sparrow.Server;
using Voron;
using Voron.Data.Tables;
using Voron.Util.Conversion;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron.Tables
{
    public unsafe class DynamicBTreeIndex : TableStorageTest
    {
        public static readonly Slice IndexName;

        public DynamicBTreeIndex(ITestOutputHelper output) : base(output)
        {
        }

        static DynamicBTreeIndex()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "DynamicBTreeIndex", ByteStringType.Immutable, out IndexName);
            }
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            base.Configure(options);

            DocsSchema.DefineIndex(new TableSchema.DynamicBTreeIndexDef
            {
                IsGlobal = true,
                IndexValueGenerator = IndexValueAction,
                Name = IndexName
            });
        }

        [Fact]
        public void CanInsertThenReadByCustom()
        {
            using (var tx = Env.WriteTransaction())
            {
                DocsSchema.Create(tx, "docs", 16);

                tx.Commit();
            }

            const string id = "users/1";
            const long etag = 1;

            using (var tx = Env.WriteTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");
                SetHelper(docs, id, "Users", etag, "{'Name': 'Oren'}");

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");

                bool gotValues = false;

                foreach (var reader in docs.SeekForwardFrom(DocsSchema.Indexes[IndexName], Slices.BeforeAllKeys, 0))
                {
                    var key = reader.Key;

                    using (Allocator.Allocate(sizeof(long) * 2, out var buffer))
                    using (Slice.From(Allocator, id, out var idSlice))
                    using (Slice.From(Allocator, EndianBitConverter.Big.GetBytes(etag * 100), out var expectedEtag))
                    {
                        expectedEtag.CopyTo(buffer.Ptr);

                        var expectedHash = Hashing.XXHash64.Calculate(idSlice.Content.Ptr, (ulong)(idSlice.Size));
                        *(long*)(buffer.Ptr + sizeof(long)) = (long)expectedHash;

                        var expected = new Slice(buffer);

                        Assert.True(SliceComparer.AreEqual(expected, key));
                    }

                    var handle = reader.Result.Reader;
                    Assert.Equal("{'Name': 'Oren'}", Encoding.UTF8.GetString(handle.Read(3, out int size), size));

                    tx.Commit();
                    gotValues = true;
                    break;
                }

                Assert.True(gotValues);
            }
        }

        [Fact]
        public void CanInsertThenDeleteByCustom()
        {
            using (var tx = Env.WriteTransaction())
            {
                DocsSchema.Create(tx, "docs", 16);

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

                Slice.From(tx.Allocator, "users/1", out Slice key);
                docs.DeleteByKey(key);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");

                var reader = docs.SeekForwardFrom(DocsSchema.Indexes[IndexName], Slices.BeforeAllKeys, 0);
                Assert.Empty(reader);
            }
        }

        [Fact]
        public void CanInsertThenUpdateByCustom()
        {
            using (var tx = Env.WriteTransaction())
            {
                DocsSchema.Create(tx, "docs", 16);

                tx.Commit();
            }

            const string id = "users/1";
            using (var tx = Env.WriteTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");
                SetHelper(docs, id, "Users", 1L, "{'Name': 'Oren'}");

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");

                SetHelper(docs, id, "Users", 2L, "{'Name': 'Eini'}");

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");

                bool gotValues = false;
                foreach (var reader in docs.SeekForwardFrom(DocsSchema.Indexes[IndexName], Slices.BeforeAllKeys, 0))
                {
                    var key = reader.Key;

                    using (Allocator.Allocate(sizeof(long) * 2, out var buffer))
                    using (Slice.From(Allocator, id, out var idSlice))
                    using (Slice.From(Allocator, EndianBitConverter.Big.GetBytes(200L), out var expectedEtag))
                    {
                        expectedEtag.CopyTo(buffer.Ptr);

                        var expectedHash = Hashing.XXHash64.Calculate(idSlice.Content.Ptr, (ulong)(idSlice.Size));
                        *(long*)(buffer.Ptr + sizeof(long)) = (long)expectedHash;

                        var expected = new Slice(buffer);

                        Assert.True(SliceComparer.AreEqual(expected, key));
                    }

                    var handle = reader.Result;
                    Assert.Equal("{'Name': 'Eini'}", Encoding.UTF8.GetString(handle.Reader.Read(3, out int size), size));
                    tx.Commit();
                    gotValues = true;
                    break;
                }

                Assert.True(gotValues);
            }
        }

        internal static ByteStringContext.Scope IndexValueAction(ByteStringContext context, ref TableValueReader tvr, out Slice slice)
        {
            var scope = context.Allocate(sizeof(long) * 2, out var buffer);

            var idPtr = tvr.Read(0, out var size);
            var hash = Hashing.XXHash64.Calculate(idPtr, (ulong)size);

            var etagPtr = tvr.Read(2, out _);
            var modifiedEtag = *(long*)etagPtr * 100;

            *(long*)buffer.Ptr = modifiedEtag;
            *(long*)(buffer.Ptr + sizeof(long)) = (long)hash;

            slice = new Slice(buffer);
            return scope;
        }
    }
}
