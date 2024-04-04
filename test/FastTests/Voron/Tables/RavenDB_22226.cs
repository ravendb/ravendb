using System;
using System.Runtime.InteropServices;
using Sparrow.Server;
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

        [Fact]
        public void CanInsertUpdateThenReadByDynamic()
        {
            using (var tx = Env.WriteTransaction())
            {
                Slice.From(tx.Allocator, "RevisionsChangeVector", ByteStringType.Immutable, out var changeVectorSlice);
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

                var cv = Guid.NewGuid().ToString();

                revisionsSchema.Create(tx, "users", 32);

                var usersTbl = tx.OpenTable(revisionsSchema, "users");

                using (usersTbl.Allocate(out var builder))
                using (Slice.From(tx.Allocator, cv, out var key))
                {
                    builder.Add(key);
                    builder.Add(0L);

                    usersTbl.Insert(builder);
                }

                using (usersTbl.Allocate(out var builder))
                using (Slice.From(tx.Allocator, cv, out var key))
                {
                    usersTbl.ReadByKey(key, out var tvr);
                    builder.Add(key);
                    builder.Add(322L);
                    usersTbl.Update(tvr.Id, builder);
                }

                foreach (var x in usersTbl.SeekForwardFrom(index, Slices.BeforeAllKeys, 0))
                {
                    var cv1 = x.Result.Reader.ReadString(0);
                    var etag1 = x.Result.Reader.ReadLong(1);

                    Assert.Equal(cv, cv1);
                    Assert.Equal(322, etag1);
                }
            }
        }

        [StorageIndexEntryKeyGenerator]
        internal static ByteStringContext.Scope IndexCvEtagKeyGenerator(Transaction tx, ref TableValueReader tvr, out Slice slice)
        {
            var cvPtr = tvr.Read(0, out var cvSize);
            var etag = tvr.ReadLong(1);

            var scope = tx.Allocator.Allocate(sizeof(long) + cvSize, out var buffer);

            var span = new Span<byte>(buffer.Ptr, buffer.Length);
            MemoryMarshal.AsBytes(new Span<long>(ref etag)).CopyTo(span);
            new ReadOnlySpan<byte>(cvPtr, cvSize).CopyTo(span[sizeof(long)..]);

            slice = new Slice(buffer);
            return scope;
        }


    }
}
