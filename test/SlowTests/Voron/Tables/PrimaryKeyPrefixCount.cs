using System.Collections.Generic;
using FastTests.Voron;
using Sparrow.Server;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Tables;
using Xunit;

namespace SlowTests.Voron.Tables
{
    public class PrimaryKeyPrefixCount(ITestOutputHelper output) : StorageTest(output)
    {
        // Count() adds whole leaves at once, it must match the number of results of the enumeration
        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData("", "", 0)]
        [InlineData("users/", "", 0)]
        [InlineData("users/", "", 7)]
        [InlineData("users/", "users/00001000", 0)]
        [InlineData("users/", "users/00001000", 25)]
        [InlineData("users/", "users/00001000/", 0)] // after a key that is not in the table
        [InlineData("users/0000", "", 0)]
        [InlineData("users/00012", "", 0)]
        [InlineData("orders/", "", 0)]
        [InlineData("users/", "", 1_000_000)]
        [InlineData("missing/", "", 0)]
        public void CountMatchesTheEnumeration(string prefix, string startAfter, long skip)
        {
            Slice docs;
            using (StorageEnvironment.GetStaticContext(out var ctx))
                Slice.From(ctx, "Docs", ByteStringType.Immutable, out docs);

            var schema = new TableSchema().DefineKey(new TableSchema.IndexDef { StartIndex = 0, Count = 1 });

            using (var tx = Env.WriteTransaction())
            {
                schema.Create(tx, docs, 16);
                var table = tx.OpenTable(schema, docs);
                var builder = new TableValueBuilder();
                var payload = new byte[40];

                // enough keys for many leaves, with other prefixes before and after
                var keys = new List<string>();
                for (int i = 0; i < 3_000; i++)
                    keys.Add($"orders/{i:D8}");
                for (int i = 0; i < 20_000; i++)
                    keys.Add($"users/{i:D8}");
                for (int i = 0; i < 3_000; i++)
                    keys.Add($"zebras/{i:D8}");

                foreach (var key in keys)
                {
                    using (Slice.From(tx.Allocator, key, out Slice k))
                    {
                        builder.Reset();
                        builder.Add(k);
                        unsafe
                        {
                            fixed (byte* p = payload)
                            {
                                builder.Add(p, payload.Length);
                                table.Set(builder);
                            }
                        }
                    }
                }

                using (Slice.From(tx.Allocator, prefix, out Slice prefixSlice))
                using (Slice.From(tx.Allocator, startAfter, out Slice startAfterSlice))
                {
                    var after = startAfter.Length == 0 ? Slices.Empty : startAfterSlice;

                    long enumerated = 0;
                    foreach (var _ in table.SeekByPrimaryKeyPrefix(prefixSlice, after, skip))
                        enumerated++;

                    Assert.Equal(enumerated, table.SeekByPrimaryKeyPrefix(prefixSlice, after, skip).Count());
                }
            }
        }
    }
}
