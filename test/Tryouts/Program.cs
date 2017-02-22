using System;
using System.Linq;
using System.Threading.Tasks;
using Lucene.Net.Store;
using Sparrow.Utils;
using Voron;
using Directory = System.IO.Directory;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
        
            if (Directory.Exists("local"))
                Directory.Delete("local", true);

            var storageEnvironmentOptions = StorageEnvironmentOptions.ForPath("local");
            storageEnvironmentOptions.ManualFlushing = true;
            using (var env = new StorageEnvironment(storageEnvironmentOptions))
            {
                int i = 0;
                while (true)
                {
                    Console.Write($"\r{NativeMemory.FileMapping.Values.Sum(x=>x.Sum(a=>a.Value))/1024:#,#} kb {i:#,#}");
                    i++;

                    var tx1 = env.WriteTransaction();
                    var tx1Tree = tx1.CreateTree("Test");
                    tx1Tree.Add("test"+i, new byte[1024 * 96]);

                    var tx2 = tx1.BeginAsyncCommitAndStartNewTransaction();
                    var tx2Tree = tx2.CreateTree("Test");
                    tx2Tree.Add("test2" + i, new byte[1024 * 128]);

                    tx1.EndAsyncCommit();
                    tx1.Dispose();

                    var tx3 = tx2.BeginAsyncCommitAndStartNewTransaction();

                    var tx3Tree = tx3.CreateTree("Test");
                    tx3Tree.Add("test3" + i, new byte[1024 * 512]);

                    tx2.EndAsyncCommit();
                    tx2.Dispose();

                    tx3.Commit();
                    tx3.Dispose();

                    env.FlushLogToDataFile();
                }

            }
        }
    }
}