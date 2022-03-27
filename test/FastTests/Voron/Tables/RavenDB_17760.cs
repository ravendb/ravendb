using System.Collections.Generic;
using System.Linq;
using System.Text;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Server;
using Voron;
using Voron.Data.Tables;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron.Tables
{
    public unsafe class RavenDB_17760 : TableStorageTest
    {
        public static readonly Slice IndexName;

        public RavenDB_17760(ITestOutputHelper output) : base(output)
        {
        }

        static RavenDB_17760()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "DynamicKeyIndex", ByteStringType.Immutable, out IndexName);
            }
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            base.Configure(options);

            DocsSchema.DefineIndex(new TableSchema.DynamicKeyIndexDef
            {
                IsGlobal = true,
                GenerateKey = IndexKeyGenerator,
                Name = IndexName
            });
        }

        [Fact]
        public void CanInsertThenReadByDynamic()
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

                foreach (var reader in docs.SeekForwardFrom(DocsSchema.DynamicKeyIndexes[IndexName], Slices.BeforeAllKeys, 0))
                {
                    AssertKey(id, etag, reader.Key);

                    var handle = reader.Result.Reader;
                    Assert.Equal("{'Name': 'Oren'}", handle.ReadString(3));

                    gotValues = true;
                    break;
                }

                Assert.True(gotValues);
            }
        }

        [Fact]
        public void CanInsertThenDeleteByDynamic()
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

                var reader = docs.SeekForwardFrom(DocsSchema.DynamicKeyIndexes[IndexName], Slices.BeforeAllKeys, 0);
                Assert.Empty(reader);
            }
        }

        [Fact]
        public void CanInsertThenUpdateByDynamic()
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
                foreach (var reader in docs.SeekForwardFrom(DocsSchema.DynamicKeyIndexes[IndexName], Slices.BeforeAllKeys, 0))
                {
                    AssertKey(id, etag: 2, reader.Key);

                    var handle = reader.Result;
                    Assert.Equal("{'Name': 'Eini'}", handle.Reader.ReadString(3));
                    gotValues = true;
                    break;
                }

                Assert.True(gotValues);
            }
        }

        [Fact]
        public void CanUpdateByDynamic_ManyItems()
        {
            using (var tx = Env.WriteTransaction())
            {
                DocsSchema.Create(tx, "docs", 16);

                tx.Commit();
            }

            const string idPrefix = "users/";

            for (long i = 0; i < 10_000; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var docs = tx.OpenTable(DocsSchema, "docs");
                    var id = $"{idPrefix}/{i}";

                    SetHelper(docs, id, "Users", i, $"{{'Name': 'Oren-{i}'}}");
                    tx.Commit();
                }
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");

                long count = 0;
                foreach (var item in docs.SeekForwardFrom(DocsSchema.DynamicKeyIndexes[IndexName], Slices.BeforeAllKeys, 0))
                {
                    var handle = item.Result;
                    
                    var id = handle.Reader.ReadString(0);
                    var etag = Bits.SwapBytes(handle.Reader.ReadLong(2));

                    AssertKey(id, etag, item.Key);

                    Assert.Equal($"{{'Name': 'Oren-{etag}'}}", handle.Reader.ReadString(3));
                    count++;
                }

                Assert.Equal(10_000, count);
            }
        }

        [Fact]
        public void CanSeekByPrefix()
        {
            using (var tx = Env.WriteTransaction())
            {
                DocsSchema.Create(tx, "docs", 16);

                tx.Commit();
            }

            const string idPrefix = "users/";
            const int bucketToCheck = 20;
            const long startEtag = 51;

            var bucketInfo = new Dictionary<string, long>();

            for (long i = 0; i < 1000; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var docs = tx.OpenTable(DocsSchema, "docs");
                    var id = $"{idPrefix}/{i}";
                    SetHelper(docs, id, "Users", i, $"{{'Name': 'Oren-{i}'}}");

                    AddToDictionaryIfNeeded(id, i);

                    tx.Commit();
                }
            }

            void AddToDictionaryIfNeeded(string s, long etag)
            {
                var strBytes = Encoding.UTF8.GetBytes(s);
                var expectedHash = Hashing.XXHash64.Calculate(strBytes, strBytes.Length);
                var expectedBucket = (int)(expectedHash % 100);

                if (expectedBucket != bucketToCheck)
                    return;

                var expectedEtag = etag * 100;

                bucketInfo.Add(s, expectedEtag);
            }

            using (var tx = Env.ReadTransaction())
            using (Allocator.Allocate(sizeof(long) + sizeof(int), out var buffer))
            {
                *(int*)buffer.Ptr = bucketToCheck;
                * (long*)(buffer.Ptr + sizeof(int)) = Bits.SwapBytes(startEtag * 100);

                using (Slice.External(Allocator, buffer, buffer.Length, out var keySlice))
                using (Slice.External(Allocator, buffer, buffer.Length - sizeof(long), out var prefix))
                {
                    var docs = tx.OpenTable(DocsSchema, "docs");

                    long prevEtag = -1;
                    long count = 0;
                    foreach (var reader in docs.SeekForwardFromPrefix(DocsSchema.DynamicKeyIndexes[IndexName], keySlice, prefix, skip: 0))
                    {
                        var b = *(int*)reader.Key.Content.Ptr;
                        Assert.Equal(bucketToCheck, b);

                        var handle = reader.Result;
                        long etag = Bits.SwapBytes(handle.Reader.ReadLong(2));

                        if (prevEtag == -1)
                            Assert.True(etag >= startEtag);
                        else
                            Assert.True(etag > prevEtag);

                        prevEtag = etag;

                        var id = handle.Reader.ReadString(0);

                        Assert.True(bucketInfo.TryGetValue(id, out var expectedEtag));
                        Assert.Equal(expectedEtag, etag * 100);

                        var data = handle.Reader.ReadString(3);
                        Assert.Equal($"{{'Name': 'Oren-{etag}'}}", data);
                        count++;

                    }

                    var expectedCount = bucketInfo.Values.Count(etag => etag >= startEtag * 100);
                    Assert.Equal(expectedCount, count);
                }
            }
        }

        private void AssertKey(string id, long etag, Slice key)
        {
            using (Allocator.Allocate(sizeof(long) + sizeof(int), out var buffer))
            using (Slice.From(Allocator, id, out var idSlice))
            {
                var expectedHash = Hashing.XXHash64.Calculate(idSlice.Content.Ptr, (ulong)idSlice.Size);
                var expectedBucket = (int)(expectedHash % 100);
                var expectedEtag = etag * 100;

                *(int*)buffer.Ptr = expectedBucket;
                *(long*)(buffer.Ptr + sizeof(int)) = Bits.SwapBytes(expectedEtag);

                var expected = new Slice(buffer);

                Assert.True(SliceComparer.AreEqual(expected, key));
            }
        }

        [StorageIndexEntryKeyGenerator]
        internal static ByteStringContext.Scope IndexKeyGenerator(ByteStringContext context, ref TableValueReader tvr, out Slice slice)
        {
            var scope = context.Allocate(sizeof(long) + sizeof(int), out var buffer);

            var idPtr = tvr.Read(0, out var size);
            var hash = Hashing.XXHash64.Calculate(idPtr, (ulong)size);
            var bucket = (int)(hash % 100);

            var etagPtr = tvr.Read(2, out _);
            var modifiedEtag = Bits.SwapBytes(*(long*)etagPtr) * 100;

            *(int*)buffer.Ptr = bucket;
            *(long*)(buffer.Ptr + sizeof(int)) = Bits.SwapBytes(modifiedEtag);

            slice = new Slice(buffer);
            return scope;
        }
    }
}
