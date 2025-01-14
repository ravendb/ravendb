using System;
using System.Linq;
using Raven.Server.Documents;
using Raven.Server.Documents.Revisions;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Tables;
using Xunit;
using Xunit.Abstractions;
using Voron.Impl;
using static Voron.Data.Tables.TableSchema;
using Sparrow.Binary;
using Sparrow.Threading;
using FastTests.Voron;
using FastTests.Voron.FixedSize;
using Sparrow.Json;
using System.Collections.Generic;

namespace SlowTests.Issues
{
    public class RavenDB_23167_Voron : StorageTest
    {
        public RavenDB_23167_Voron(ITestOutputHelper output) : base(output)
        {
        }
        
        [RavenFact(RavenTestCategory.Voron)]
        public void SeekBackwardFrom_VoronOverloads()
        {
            RequireFileBasedPager();

            using (var allocator = new ByteStringContext(SharedMultipleUseFlag.None))
            using (Slice.From(allocator, "PK", out var pk))
            using (Slice.From(allocator, "Etags", out var etagsSlice))
            using (Slice.From(allocator, "RevisionsIdAndEtag", out var idAndEtagSlice))
            using (Slice.From(allocator, "Table", out var tableName))
            {
                var etagsIndex = new FixedSizeSchemaIndexDef { Name = etagsSlice, IsGlobal = false, StartIndex = 0 };
                var idAndEtagIndex = new SchemaIndexDef { StartIndex = 1, Count = 3, Name = idAndEtagSlice };
                var schema = new TableSchema()
                    .DefineKey(new SchemaIndexDef { Name = pk, IsGlobal = false, StartIndex = 0, Count = 1 })
                    .DefineFixedSizeIndex(etagsIndex)
                    .DefineIndex(idAndEtagIndex);

                using (var tx = Env.WriteTransaction())
                {
                    schema.Create(tx, tableName, null);
                    tx.Commit();
                }

                using (var tx = Env.WriteTransaction())
                {
                    using var table = tx.OpenTable(schema, tableName);

                    InsertToTable(tx, table, 2, "foo/bar");
                    InsertToTable(tx, table, 4, "foo/bar1");
                    InsertToTable(tx, table, 6, "foo/bar");

                    tx.Commit();
                }

                using (var tx = Env.ReadTransaction())
                {
                    using var table = tx.OpenTable(schema, tableName);

                    AssertSeekBackwardForFixedSizeTrees(table, etagsIndex, endEtag: 3, empty: false, expectedEtag: 2);
                    AssertSeekBackwardForFixedSizeTrees(table, etagsIndex, endEtag: 0, empty: true);
                    AssertSeekBackwardForFixedSizeTrees(table, etagsIndex, endEtag: 10, empty: false, expectedEtag: 6);
                    AssertSeekBackwardForFixedSizeTrees(table, etagsIndex, endEtag: long.MaxValue, empty: false, expectedEtag: 6);

                    AssertSeekBackward(tx.Allocator, table, idAndEtagIndex, "foo/bar", endEtag: 3, empty: false, expectedEtag: 2);
                    AssertSeekBackward(tx.Allocator, table, idAndEtagIndex, "foo/bar", endEtag: 0, empty: true);
                    AssertSeekBackward(tx.Allocator, table, idAndEtagIndex, "foo/bar", endEtag: 10, empty: false, expectedEtag: 6);
                    AssertSeekBackward(tx.Allocator, table, idAndEtagIndex, "foo/bar", endEtag: long.MaxValue, empty: false, expectedEtag: 6);

                    AssertSeekBackward(tx.Allocator, table, idAndEtagIndex, "foo/bar", endEtag: 5, empty: false, expectedEtag: 2);
                    AssertSeekBackward(tx.Allocator, table, idAndEtagIndex, "foo/bar1", endEtag: 5, empty: false, expectedEtag: 4);
                    AssertSeekBackward(tx.Allocator, table, idAndEtagIndex, "foo/ba", endEtag: 5, empty: true);

                    AssertSeekBackwardAfterAllKeys(tx.Allocator, table, idAndEtagIndex, "foo/bar", empty: false, expectedEtag: 6);
                    AssertSeekBackwardAfterAllKeys(tx.Allocator, table, idAndEtagIndex, "foo/ba", empty: true);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineDataWithRandomSeed(10)]
        [InlineDataWithRandomSeed(100)]
        public void SeekBackwardFrom_VoronOverloads_Random(int numberOfEntities, int seed)
        {
            RequireFileBasedPager();

            var r = new Random(seed);

            using (var allocator = new ByteStringContext(SharedMultipleUseFlag.None))
            using (Slice.From(allocator, "PK", out var pk))
            using (Slice.From(allocator, "Etags", out var etagsSlice))
            using (Slice.From(allocator, "RevisionsIdAndEtag", out var idAndEtagSlice))
            using (Slice.From(allocator, "Table", out var tableName))
            {
                var etagsIndex = new FixedSizeSchemaIndexDef { Name = etagsSlice, IsGlobal = false, StartIndex = 0 };
                var idAndEtagIndex = new SchemaIndexDef { StartIndex = 1, Count = 3, Name = idAndEtagSlice };
                var schema = new TableSchema()
                    .DefineKey(new SchemaIndexDef { Name = pk, IsGlobal = false, StartIndex = 0, Count = 1 })
                    .DefineFixedSizeIndex(etagsIndex)
                    .DefineIndex(idAndEtagIndex);

                using (var tx = Env.WriteTransaction())
                {
                    schema.Create(tx, tableName, null);
                    tx.Commit();
                }

                List<(string Id, long Etag)> entities;
                List<(string Id, long Etag)> users;
                List<(string Id, long Etag)> companies;

                using (var tx = Env.WriteTransaction())
                {
                    entities = new List<(string Id, long Etag)>();
                    var usedEtags = new HashSet<long>();

                    using var table = tx.OpenTable(schema, tableName);

                    for (int i = 1; i < numberOfEntities; i++)
                    {
                        string id;
                        long etag;
                        do
                        {
                            etag = r.NextInt64(numberOfEntities * 10);
                        } while (usedEtags.Contains(etag)); // Keep generating until a unique etag is found
                        usedEtags.Add(etag); // Add the unique etag to the HashSet

                        if (i % 2 == 0)
                            id = $"users/{i}";
                        else
                            id = $"companies/{i}";

                        InsertToTable(tx, table, etag, id);

                        entities.Add((id, etag));
                    }

                    tx.Commit();

                    entities = entities
                        .OrderBy(entity => entity.Etag)
                        .ToList();

                    users = entities.Where(x => x.Id.StartsWith("users/")).ToList();
                    companies = entities.Where(x => x.Id.StartsWith("companies/")).ToList();
                }

                using (var tx = Env.ReadTransaction())
                {
                    using var table = tx.OpenTable(schema, tableName);

                    // Entities:
                    var entity = entities.First();
                    AssertSeekBackwardForFixedSizeTrees(table, etagsIndex, endEtag: entity.Etag - 1, empty: true);
                    AssertSeekBackwardForFixedSizeTrees(table, etagsIndex, endEtag: entity.Etag, empty: false, entity.Etag);

                    int mid = entities.Count / 2;
                    entity = entities[mid];
                    AssertSeekBackwardForFixedSizeTrees(table, etagsIndex, endEtag: entity.Etag, empty: false, expectedEtag: entity.Etag);
                    var expectedEtag = entities[mid + 1].Etag == entities[mid].Etag + 1 ? entities[mid].Etag + 1 : entities[mid].Etag;
                    AssertSeekBackwardForFixedSizeTrees(table, etagsIndex, endEtag: entity.Etag + 1, empty: false, expectedEtag: expectedEtag);

                    entity = entities.Last();
                    AssertSeekBackwardForFixedSizeTrees(table, etagsIndex, endEtag: entity.Etag, empty: false, expectedEtag: entity.Etag);
                    AssertSeekBackwardForFixedSizeTrees(table, etagsIndex, endEtag: entity.Etag + 1, empty: false, expectedEtag: entity.Etag);

                    // Users:
                    var user = users.First();
                    AssertSeekBackward(tx.Allocator, table, idAndEtagIndex, user.Id, endEtag: user.Etag - 1, empty: true);
                    AssertSeekBackward(tx.Allocator, table, idAndEtagIndex, user.Id, endEtag: user.Etag, empty: false, expectedEtag: user.Etag);

                    mid = users.Count / 2;
                    user = users[mid];
                    AssertSeekBackward(tx.Allocator, table, idAndEtagIndex, user.Id, endEtag: user.Etag, empty: false, expectedEtag: user.Etag);
                    var userExpectedEtag = users[mid + 1].Etag == users[mid].Etag + 1 ? users[mid].Etag + 1 : users[mid].Etag;
                    AssertSeekBackward(tx.Allocator, table, idAndEtagIndex, user.Id, endEtag: user.Etag + 1, empty: false, expectedEtag: userExpectedEtag);

                    user = users.Last();
                    AssertSeekBackward(tx.Allocator, table, idAndEtagIndex, user.Id, endEtag: user.Etag, empty: false, expectedEtag: user.Etag);
                    AssertSeekBackward(tx.Allocator, table, idAndEtagIndex, user.Id, endEtag: user.Etag + 1, empty: false, expectedEtag: user.Etag);

                    // Companies:
                    var company = companies.First();
                    AssertSeekBackward(tx.Allocator, table, idAndEtagIndex, company.Id, endEtag: company.Etag - 1, empty: true);
                    AssertSeekBackward(tx.Allocator, table, idAndEtagIndex, company.Id, endEtag: company.Etag, empty: false, expectedEtag: company.Etag);

                    mid = companies.Count / 2;
                    company = companies[mid];
                    AssertSeekBackward(tx.Allocator, table, idAndEtagIndex, company.Id, endEtag: company.Etag, empty: false, expectedEtag: company.Etag);
                    var companyExpectedEtag = companies[mid + 1].Etag == companies[mid].Etag + 1 ? companies[mid].Etag + 1 : companies[mid].Etag;
                    AssertSeekBackward(tx.Allocator, table, idAndEtagIndex, company.Id, endEtag: company.Etag + 1, empty: false, expectedEtag: companyExpectedEtag);

                    company = companies.Last();
                    AssertSeekBackward(tx.Allocator, table, idAndEtagIndex, company.Id, endEtag: company.Etag, empty: false, expectedEtag: company.Etag);
                    AssertSeekBackward(tx.Allocator, table, idAndEtagIndex, company.Id, endEtag: company.Etag + 1, empty: false, expectedEtag: company.Etag);
                }
            }
        }

        private static void AssertSeekBackwardForFixedSizeTrees(Table table, FixedSizeSchemaIndexDef voronIndex, long endEtag, bool empty, long? expectedEtag = null)
        {
            var tvhs = table.SeekBackwardFrom(voronIndex, endEtag);
            var keys = tvhs.Select(tvh => DocumentsStorage.TableValueToEtag((int)TestTable.KeyEtag, ref tvh.Reader)).ToList();

            if (empty)
            {
                Assert.Empty(keys);
                if (expectedEtag.HasValue)
                    throw new InvalidOperationException("Cannot pass `expectedEtag` when `empty` is true");
            }
            else
            {
                Assert.NotEmpty(keys);
                var lastLocalKey = keys[0];
            
                if (expectedEtag.HasValue)
                    Assert.Equal(expectedEtag.Value, lastLocalKey);
                else
                    Assert.True(endEtag >= lastLocalKey, $"endEtag {endEtag}, lastLocalEtag: {lastLocalKey}");
            }
        }

        private static void AssertSeekBackward(ByteStringContext allocator, Table table, SchemaIndexDef voronIndex, 
            string id, long endEtag, bool empty, long? expectedEtag = null)
        {
            using (Slice.From(allocator, id, out var idSlice))
            using (RevisionsStorage.GetKeyPrefix(allocator, idSlice, out Slice prefixSlice))
            using (RevisionsStorage.GetKeyWithEtag(allocator, idSlice, endEtag, out var compoundPrefix))
            {
                var seekResults = table.SeekBackwardFrom(voronIndex, prefixSlice, compoundPrefix, 0).ToList();

                if (empty)
                {
                    Assert.Empty(seekResults);
                    if (expectedEtag.HasValue)
                        throw new InvalidOperationException("Cannot pass `expectedEtag` when `empty` is true");
                }
                else
                {
                    Assert.NotEmpty(seekResults);
                    using var ctx = new JsonOperationContext(4096, 16 * 1024, 32 * 1024, SharedMultipleUseFlag.None);

                    var tvr = seekResults[0].Result.Reader;

                    var lastLocalEtag = DocumentsStorage.TableValueToEtag((int)TestTable.Etag, ref tvr);
                    
                    if (expectedEtag.HasValue)
                        Assert.Equal(expectedEtag.Value, lastLocalEtag);
                    else
                        Assert.True(endEtag >= lastLocalEtag, $"endEtag {endEtag}, lastLocalEtag: {lastLocalEtag}");
                    

                    var lastLocalId = DocumentsStorage.TableValueToString(ctx, (int)TestTable.LowerId, ref tvr);
                    Assert.Equal(id, lastLocalId);

                }
            }
        }

        private static void AssertSeekBackwardAfterAllKeys(ByteStringContext allocator, Table table, SchemaIndexDef voronIndex, string id, bool empty = false, long? expectedEtag = null)
        {
            using (Slice.From(allocator, id, out var idSlice))
            using (RevisionsStorage.GetKeyPrefix(allocator, idSlice, out Slice prefixSlice))
            {
                var seekResults = table.SeekBackwardFrom(voronIndex, prefixSlice, Slices.AfterAllKeys, 0).ToList();

                if (empty)
                {
                    Assert.Empty(seekResults);
                    if (expectedEtag.HasValue)
                        throw new InvalidOperationException("Cannot pass `expectedEtag` when `empty` is true");
                }
                else
                {
                    Assert.NotEmpty(seekResults);
                    using var ctx = new JsonOperationContext(4096, 16 * 1024, 32 * 1024, SharedMultipleUseFlag.None);

                    var tvr = seekResults[0].Result.Reader;

                    if (expectedEtag.HasValue)
                    {
                        var lastLocalEtag = DocumentsStorage.TableValueToEtag((int)TestTable.Etag, ref tvr);
                        Assert.Equal(expectedEtag.Value, lastLocalEtag);
                    }

                    string lastLocalId = DocumentsStorage.TableValueToString(ctx, (int)TestTable.LowerId, ref tvr).ToString();
                    Assert.Equal(id, lastLocalId);
                }
            }
        }

        private static void InsertToTable(Transaction tx, Table table, long etag, string id)
        {
            using (Slice.From(tx.Allocator, id, ByteStringType.Immutable, out var idSlice))
            using (table.Allocate(out TableValueBuilder tvb))
            {
                tvb.Add(Bits.SwapBytes(etag));
                tvb.Add(idSlice); // Adding the same slice as the value for simplicity
                tvb.Add(SpecialChars.RecordSeparator);
                tvb.Add(Bits.SwapBytes(etag));
                table.Insert(tvb);
            }
        }

        private enum TestTable
        {
            KeyEtag = 0,
            LowerId = 1,
            RecordSeparator = 2,
            Etag = 3
        }

    }
}
