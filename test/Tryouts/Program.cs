using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using FastTests.Voron;
using Voron;
using Voron.Global;
using Voron.Impl.Scratch;
using Xunit;
using Sparrow.Platform;
using System.Linq;
using System.Text;
using FastTests.Blittable;
using FastTests.Issues;
using FastTests.Server.Documents;
using FastTests.Server.Documents.Queries;
using FastTests.Server.Replication;
using FastTests.Voron.FixedSize;
using FastTests.Voron.RawData;
using FastTests.Voron.Tables;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using SlowTests.Tests;
using SlowTests.Voron;
using Sparrow.Json;
using Sparrow.Logging;
using Voron.Data.Tables;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            if (Directory.Exists(@"C:\temp\ERL"))
                Directory.Delete(@"C:\temp\ERL", true);
            var documentDatabase = new DocumentDatabase("foo", new RavenConfiguration
            {
                Core =
                {
                    DataDirectory = @"C:\temp\ERL"
                },
            }, null);

            documentDatabase.Initialize();
            var tableSchema = new TableSchema();

            DocumentsOperationContext context;
            using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            {
                var tx1 = context.OpenWriteTransaction();

                var b = context.ReadForDisk(new MemoryStream(Encoding.UTF8.GetBytes("{}")), "adi");

                documentDatabase.DocumentsStorage.Put(context, "1", null, b);
                documentDatabase.DocumentsStorage.Put(context, "2", null, b);

                var tx2 = tx1.BeginAsyncCommitAndStartNewTransaction();
                context.Transaction = tx2;

                documentDatabase.DocumentsStorage.Put(context, "1", null, b);
                documentDatabase.DocumentsStorage.Put(context, "2", null, b);

                tx1.EndAsyncCommit();

                tx1.Dispose();

                var t2 = tx2.InnerTransaction.OpenTable(tableSchema, "Collection.Tombstones.@empty");


                var tx3 = tx2.BeginAsyncCommitAndStartNewTransaction();

                context.Transaction = tx3;

                var t3 = tx3.InnerTransaction.OpenTable(tableSchema, "Collection.Tombstones.@empty");

                tx2.EndAsyncCommit();
                tx2.Dispose();

                tx3.Commit();

                tx3.Dispose();
            }

            using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            {
                var tx1 = context.OpenWriteTransaction();

                var b = context.ReadForDisk(new MemoryStream(Encoding.UTF8.GetBytes("{}")), "adi2");

                documentDatabase.DocumentsStorage.Put(context, "1", null, b);
                documentDatabase.DocumentsStorage.Put(context, "2", null, b);

                tx1.Commit();

            }
        }
    }
}