using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq.Expressions;
using System.Runtime;
using Nevar.Debugging;
using Nevar.Impl;
using Nevar.Tests.Storage;
using Nevar.Tests.Trees;

namespace Nevar.Tryout
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            var f = File.Open(@"E:\data\bench.data",FileMode.OpenOrCreate, FileAccess.ReadWrite);
            var mmf = MemoryMappedFile.CreateFromFile(f, "test", f.Length,MemoryMappedFileAccess.ReadWrite, null, HandleInheritability.None, true);

            var memoryMappedViewAccessor = mmf.CreateViewAccessor();
           
            f.Flush(true);
            
            memoryMappedViewAccessor.Write(8192, false);

            memoryMappedViewAccessor.Write(4096*1024, true);

            memoryMappedViewAccessor.Flush();


            f.Flush(true);

            memoryMappedViewAccessor.Write(0, false);
            
            memoryMappedViewAccessor.Flush();

            f.Flush(true);

            


            //var buf = new byte[256*3];

            //using (var storage = new StorageEnvironment(new PureMemoryPager()))
            //{
            //    using (Transaction tx = storage.NewTransaction(TransactionFlags.ReadWrite))
            //    {
            //        for (int i = 0; i < 5000; i++)
            //        {
            //            storage.Root.Add(tx, i.ToString("00000"), new MemoryStream(buf));
            //        }

            //        tx.Commit();
            //    }

            //    using (Transaction tx = storage.NewTransaction(TransactionFlags.Read))
            //    {
            //        DebugStuff.RenderAndShow(tx, storage.Root.Root, 1);
            //    }


            //}
        }
    }
}