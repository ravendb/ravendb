using FastTests.Voron.Tables;
using Sparrow.Binary;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Tables;
using Xunit;
using static FastTests.Voron.Tables.RavenDB_17760;

namespace SlowTests.Voron.Tables
{
    public class RavenDB_17760(ITestOutputHelper output) : TableStorageTest(output)
    {
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

        [RavenFact(RavenTestCategory.Voron)]
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
                foreach (var item in docs.SeekByPrefix(DocsSchema.DynamicKeyIndexes[IndexName], Slices.BeforeAllKeys, Slices.Empty, 0))
                {
                    var handle = item.Result;
                    
                    var id = handle.ReadString(0);
                    var etag = Bits.SwapBytes(handle.ReadLong(2));

                    AssertKey(Allocator, id, etag, item.Key);

                    Assert.Equal($"{{'Name': 'Oren-{etag}'}}", handle.ReadString(3));
                    count++;
                }

                Assert.Equal(10_000, count);
            }
        }
    }
}
