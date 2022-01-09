using System;
using System.Text;
using FastTests.Voron;
using Sparrow.Binary;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using Voron.Data.Tables;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17758 : StorageTest
    {
        public RavenDB_17758(ITestOutputHelper output) : base(output)
        {
        }

        public string LongRandomString()
        {
            var random = new Random();
            var sb = new StringBuilder();
            for (int j = 0; j < random.Next(20,400); j++)
            {
                var key = Guid.NewGuid().ToString();
                sb.AppendLine(key);
            }

            return sb.ToString();
        }

        [Fact]
        public void WillNotRememberOldDictionariesAfterRestart()
        {
            RequireFileBasedPager();

            
            using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
            using var ____ = Slice.From(allocator, "PK", out var pk);
            using var ___ = Slice.From(allocator, "Etags", out var etags);
            using var __ = Slice.From(allocator, "Table", out var tbl);

            var idx = new TableSchema.FixedSizeSchemaIndexDef { Name = etags, IsGlobal = false, StartIndex = 0 };
            var schema = new TableSchema()
                .DefineKey(new TableSchema.SchemaIndexDef
                {
                    Name = pk,
                    IsGlobal = false,
                    StartIndex = 0,
                    Count = 1
                })
                .DefineFixedSizeIndex(idx)
                .CompressValues(idx, true);

            using (var wtx = Env.WriteTransaction())
            {
                schema.Create(wtx, tbl, null);
                wtx.Commit();
            }
            
            
            using (var wtx = Env.WriteTransaction())
            {
                var table = wtx.OpenTable(schema, tbl);

                using var _ = Slice.From(allocator,  LongRandomString(), out var val); 

                for (long i = 0; i < 1024*4; i++)
                {
                    using (table.Allocate(out var builder))
                    {
                        builder.Add(Bits.SwapBytes(i));
                        builder.Add(val);
                        table.Insert(builder);
                    }
                }
                
                wtx.Commit();
            }
            
            using (var wtx = Env.WriteTransaction())
            {
                var table = wtx.OpenTable(schema, tbl);

                using var _ = Slice.From(allocator,  LongRandomString(), out var val);

                for (long i = 20_000; i <  20_000 + 1024*4; i++)
                {
                    using (table.Allocate(out var builder))
                    {
                        builder.Add(Bits.SwapBytes(i));
                        builder.Add(val);
                        table.Insert(builder);
                    }

                    table.ReadLast(idx); // force to read the new dictionary
                }
                
                // explicitly discard the change
                //wtx.Commit();
            }
            
            using (var wtx = Env.WriteTransaction())
            {
                var table = wtx.OpenTable(schema, tbl);

                using var _ = Slice.From(allocator,  LongRandomString(), out var val);

                for (long i = 20_000; i <  20_000 +  1020*4; i++)
                {
                    using (table.Allocate(out var builder))
                    {
                        builder.Add(Bits.SwapBytes(i));
                        builder.Add(val);
                        table.Insert(builder);
                    }
                }

                wtx.Commit();
            }
            
            RestartDatabase();
            
            using (var rtx = Env.ReadTransaction())
            {
                var table = rtx.OpenTable(schema, tbl);

                using (var it = table.SeekForwardFrom(idx, 0, 0).GetEnumerator())
                {
                    while (it.MoveNext())
                    {
                        
                    }
                }
            }

        }
    }
}
