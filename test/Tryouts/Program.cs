using System;
using System.IO;
using System.Text;
using Sparrow.Logging;
using Voron;
using Voron.Data.Tables;

namespace Tryout
{
    internal class Program
    {
        private unsafe static void Main(string[] args)
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly(),
                    new LoggerSetup(Path.GetTempPath(), LogMode.None)))
            {
                var tableSchema = new TableSchema
                {
                    AllowNoIndexesOrPrimaryKey = true
                };

                using (var tx = env.WriteTransaction())
                {
                    tableSchema.Create(tx, "Nodes");

                    tableSchema.Create(tx, "Edges");

                    tx.Commit();
                }

                long id;

                using (var tx = env.WriteTransaction())
                {
                    var table = new Table(tableSchema, "Nodes", tx);
                    var bytes = Encoding.UTF8.GetBytes("Oren");
                    fixed (byte* b = bytes)
                    {
                        id = table.Insert(new TableValueBuilder
                        {
                            {b, bytes.Length}
                        });
                    }
                    tx.Commit();
                }

                using (var tx = env.ReadTransaction())
                {
                    var table = new Table(tableSchema, "Nodes", tx);
                    int size;
                    var reader = new TableValueReader(table.DirectRead(id, out size), size);
                    var data = reader.Read(0, out size);
                    Console.WriteLine(Encoding.UTF8.GetString(data, size));
                    tx.Commit();
                }
            }
        }
    }
}