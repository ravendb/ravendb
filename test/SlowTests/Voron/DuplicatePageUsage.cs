using System.IO;
using System.Reflection;
using FastTests;
using Voron;
using Voron.Data.Tables;
using Xunit;

namespace SlowTests.Voron
{
    public class DuplicatePageUsage : NoDisposalNeeded
    {

        private readonly TableSchema _entriesSchema = new TableSchema()
           .DefineKey(new TableSchema.SchemaIndexDef
           {
               StartIndex = 0,
               Count = 1
           });

        [Fact]
        public unsafe void ShouldNotHappen()
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
            {
                using (var tx = env.WriteTransaction())
                {
                    tx.CreateTree("Fields");
                    tx.CreateTree("Options");
                    _entriesSchema.Create(tx, "IndexEntries", 16);
                    tx.Commit();
                }

                using (var tx = env.WriteTransaction())
                {
                    var options = tx.CreateTree("Options");
                    var entries = tx.OpenTable(_entriesSchema, "IndexEntries");
                    for (int i = 0; i < 10; i++)
                    {
                        var assembly = typeof(DuplicatePageUsage).GetTypeInfo().Assembly;
                        fixed (byte* buffer = new byte[1024])
                        using (var fs = assembly.GetManifestResourceStream("SlowTests.Data.places.txt"))
                        using (var reader = new StreamReader(fs))
                        {
                            string readLine;
                            while ((readLine = reader.ReadLine()) != null)
                            {
                                var strings = readLine.Split(',');
                                var id = long.Parse(strings[0]);
                                var size = int.Parse(strings[1]);
                                entries.Set(new TableValueBuilder
                                {
                                    {(byte*) &id, sizeof (int)},
                                    {buffer, size}
                                });

                            }
                        }
                    }

                    var val = long.MaxValue/2;
                    Slice key, v;
                    Slice.From(tx.Allocator, "LastEntryId", out key);
                    Slice.From(tx.Allocator, (byte*) &val, sizeof(long), out v);
                    options.Add(key, v);
                    tx.Commit();
                }
            }
        }
    }
}