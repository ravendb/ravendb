using System;
using System.Collections.Generic;
using System.Text;
using Sparrow;
using Voron;
using Voron.Data.Tables;
using Xunit;

namespace FastTests.Voron.Bugs
{
    public class GlobalFixedSizeTreeInMulitpleTables : StorageTest
    {
        [Fact]
        public unsafe void CanBeSafelyModifiedOnEither()
        {
            using (var tx = Env.WriteTransaction())
            {
                Slice.From(tx.Allocator, "RevisionsChangeVector", ByteStringType.Immutable, out var changeVectorSlice);
                Slice.From(tx.Allocator, "Etag", ByteStringType.Immutable, out var etag);
                var revisionsSchema = new TableSchema();
                revisionsSchema.DefineKey(new TableSchema.SchemaIndexDef
                {
                    StartIndex = 0,
                    Count = 1,
                    Name = changeVectorSlice,
                    IsGlobal = false
                });
                var indexDef = new TableSchema.SchemaIndexDef
                {
                    StartIndex = 1,
                    Name = etag,
                    IsGlobal = true
                };
                revisionsSchema.DefineIndex(indexDef);

                revisionsSchema.Create(tx, "users", 32);
                revisionsSchema.Create(tx, "people", 32);

                var usersTbl = tx.OpenTable(revisionsSchema, "users");
                var peopleTbl = tx.OpenTable(revisionsSchema, "people");

                using (usersTbl.Allocate(out var builder))
                using (Slice.From(tx.Allocator, Guid.NewGuid().ToString(), out var key))
                {
                    builder.Add(key);
                    builder.Add(0L);

                    usersTbl.Insert(builder);
                }

                for (int i = 0; i < 127; i++)
                {
                    using (peopleTbl.Allocate(out var builder))
                    using (Slice.From(tx.Allocator, Guid.NewGuid().ToString(), out var key))
                    {
                        builder.Add(key);
                        builder.Add(0L);

                        peopleTbl.Insert(builder);
                    }
                }

                using (peopleTbl.Allocate(out var builder))
                using (Slice.From(tx.Allocator, Guid.NewGuid().ToString(), out var key))
                {
                    builder.Add(key);
                    builder.Add(0L);

                    peopleTbl.Insert(builder);
                }

                using (Slice.From(tx.Allocator, new byte[8], out var empty))
                {
                    var userIndex = usersTbl.GetFixedSizeTree(usersTbl.GetTree(indexDef), empty, 0, true);
                    var peopleIndex = peopleTbl.GetFixedSizeTree(usersTbl.GetTree(indexDef), empty, 0, true);

                    Assert.Equal(userIndex.NumberOfEntries, peopleIndex.NumberOfEntries);
                    Assert.Equal(userIndex.Type, peopleIndex.Type);
                }
            }
        }
    }
}
