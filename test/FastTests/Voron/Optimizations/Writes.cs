// -----------------------------------------------------------------------
//  <copyright file="Foo.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;
using System.Text;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Xunit;
using Voron.Data.Tables;
using Voron.Debugging;

namespace FastTests.Voron.Optimizations
{
    public class Writes : StorageTest
    {
        [Fact]
        public void EarlyLockRelease()
        {
            var configuration = new RavenConfiguration("foo", ResourceType.Database);
            configuration.Initialize();

            configuration.Core.RunInMemory = true;
            configuration.Core.DataDirectory = new PathSetting(Path.GetTempPath() + @"\elr");

            var documentDatabase = new DocumentDatabase("foo", configuration, null);
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

                tx2.InnerTransaction.OpenTable(tableSchema, "Collection.Tombstones.@empty");
                var tx3 = tx2.BeginAsyncCommitAndStartNewTransaction();
                context.Transaction = tx3;

                tx3.InnerTransaction.OpenTable(tableSchema, "Collection.Tombstones.@empty");
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

        [Fact]
        public void SinglePageModificationDoNotCauseCopyingAllIntermediatePages()
        {
            var keySize = 1024;
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                tree.Add(new string('9', keySize), new MemoryStream(new byte[3]));
                DebugStuff.RenderAndShow(tree);
                tree.Add(new string('1', keySize), new MemoryStream(new byte[3]));
                DebugStuff.RenderAndShow(tree);
                tree.Add(new string('4', 1000), new MemoryStream(new byte[2]));
                DebugStuff.RenderAndShow(tree);
                tree.Add(new string('5', keySize), new MemoryStream(new byte[2]));
                DebugStuff.RenderAndShow(tree);
                tree.Add(new string('8', keySize), new MemoryStream(new byte[3]));
                DebugStuff.RenderAndShow(tree);
                tree.Add(new string('2', keySize), new MemoryStream(new byte[2]));
                DebugStuff.RenderAndShow(tree);
                tree.Add(new string('6', keySize), new MemoryStream(new byte[2]));
                DebugStuff.RenderAndShow(tree);
                tree.Add(new string('0', keySize), new MemoryStream(new byte[4]));
                DebugStuff.RenderAndShow(tree);
                tree.Add(new string('3', 1000), new MemoryStream(new byte[1]));
                DebugStuff.RenderAndShow(tree);
                tree.Add(new string('7', keySize), new MemoryStream(new byte[1]));
                
                tx.Commit();
            }

            var afterAdds = Env.NextPageNumber;

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                tree.Delete(new string('0', keySize));

                tree.Add(new string('4', 1000), new MemoryStream(new byte[21]));

                tx.Commit();
            }

            Assert.Equal(afterAdds, Env.NextPageNumber);

            // ensure changes were applied
            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.CreateTree("foo");
                Assert.Null(tree.Read(new string('0', keySize)));

                var readResult = tree.Read(new string('4', 1000));

                Assert.Equal(21, readResult.Reader.Length);
            }
        }
    }
}