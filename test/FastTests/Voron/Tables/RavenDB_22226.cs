using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Sparrow.Server;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron.Tables
{
    public unsafe class RavenDB_22226 : TableStorageTest
    {
        public static readonly Slice IndexName;

        public RavenDB_22226(ITestOutputHelper output) : base(output)
        {
        }

        static RavenDB_22226()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "DynamicKeyIndex", ByteStringType.Immutable, out IndexName);
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void CanInsertUpdateThenReadByDynamic()
        {
            using (var tx = Env.WriteTransaction())
            {
                Slice.From(tx.Allocator, "RevisionsChangeVector", ByteStringType.Immutable, out var changeVectorSlice);
                Slice.From(tx.Allocator, "DocumentsChangeVector", ByteStringType.Immutable, out var documentsSlice);
                Slice.From(tx.Allocator, "Etag", ByteStringType.Immutable, out var etag);
                var revisionsSchema = new TableSchema();
                revisionsSchema.DefineKey(new TableSchema.IndexDef
                {
                    StartIndex = 0,
                    Count = 1,
                    Name = changeVectorSlice,
                    IsGlobal = false
                });
                var index = new TableSchema.DynamicKeyIndexDef { IsGlobal = true, GenerateKey = IndexCvEtagKeyGenerator, Name = IndexName, SupportDuplicateKeys = true };
                revisionsSchema.DefineIndex(index);

                var cv1 = Guid.NewGuid().ToString();
                var cv2 = Guid.NewGuid().ToString();
                var cv3 = Guid.NewGuid().ToString();
                var shared = Guid.NewGuid().ToString();

                revisionsSchema.Create(tx, "users", 32);

                var usersTbl = tx.OpenTable(revisionsSchema, "users");

                PopulateTable(tx, usersTbl, cv1, shared, 0L);
                PopulateTable(tx, usersTbl, cv2, shared, 0L);
                PopulateTable(tx, usersTbl, cv3, shared, 228L);

                using (usersTbl.Allocate(out var builder))
                using (Slice.From(tx.Allocator, cv1, out var key))
                {
                    usersTbl.ReadByKey(key, out var tvr);
                    var ptr = tvr.Read(1, out int size);
                    using var x = Slice.From(tx.Allocator, ptr, size, ByteStringType.Immutable, out var sharedSlice);

                    builder.Add(key);
                    builder.Add(sharedSlice);
                    builder.Add(322L);
                    usersTbl.Update(tvr.Id, builder);
                }

                var results = new List<(string key, string shared, long etag)>();
                foreach (var x in usersTbl.SeekForwardFrom(index, Slices.BeforeAllKeys, 0))
                {
                    results.Add((x.Result.Reader.ReadString(0), x.Result.Reader.ReadString(1), x.Result.Reader.ReadLong(2)));
                }

                //Assert results
                Assert.Equal(3, results.Count);

                Assert.Contains(results, x=> x.key == cv1 && x.shared == shared && x.etag == 322L);
                Assert.Contains(results, x=> x.key == cv2 && x.shared == shared && x.etag == 0L);
                Assert.Contains(results, x=> x.key == cv3 && x.shared == shared && x.etag == 228L);

                using (Slice.From(tx.Allocator, cv1, out var key))
                {
                    usersTbl.ReadByKey(key, out var tvr);
                    usersTbl.Delete(tvr.Id);
                }

                results = new List<(string key, string shared, long etag)>();
                foreach (var x in usersTbl.SeekForwardFrom(index, Slices.BeforeAllKeys, 0))
                {
                    results.Add((x.Result.Reader.ReadString(0), x.Result.Reader.ReadString(1), x.Result.Reader.ReadLong(2)));
                }

                //Assert results
                Assert.Equal(2, results.Count);
                Assert.Contains(results, x => x.key == cv2 && x.shared == shared && x.etag == 0L);
                Assert.Contains(results, x => x.key == cv3 && x.shared == shared && x.etag == 228L);
            }
        }

        private void PopulateTable(Transaction tx, Table usersTbl, string cv, string shared, long etag)
        {
            using (usersTbl.Allocate(out var builder))
            using (Slice.From(tx.Allocator, cv, out var key))
            using (Slice.From(tx.Allocator, shared, out var sharedSlice))
            {
                builder.Add(key);
                builder.Add(sharedSlice);
                builder.Add(etag);

                usersTbl.Insert(builder);
            }
        }

        [StorageIndexEntryKeyGenerator]
        internal static ByteStringContext.Scope IndexCvEtagKeyGenerator(Transaction tx, ref TableValueReader tvr, out Slice slice)
        {
            var cvPtr = tvr.Read(1, out var cvSize);
            var etag = tvr.ReadLong(2);

            var scope = tx.Allocator.Allocate(sizeof(long) + cvSize, out var buffer);

            var span = new Span<byte>(buffer.Ptr, buffer.Length);
            MemoryMarshal.AsBytes(new Span<long>(ref etag)).CopyTo(span);
            new ReadOnlySpan<byte>(cvPtr, cvSize).CopyTo(span[sizeof(long)..]);

            slice = new Slice(buffer);
            return scope;
        }
    }
}
