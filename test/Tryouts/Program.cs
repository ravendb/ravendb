using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using FastTests.Blittable;
using FastTests.Client;
using FastTests.Voron.Sets;
using SlowTests.Issues;
using SlowTests.MailingList;
using SlowTests.Server.Documents.ETL.Raven;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Sets;

namespace Tryouts
{
    public static class Program
    {
        static Program()
        {
            XunitLogging.RedirectStreams = false;
        }

        public static unsafe void Main()
        {
            using (var t = new SetTests(new ConsoleTestOutputHelper()))
            {
                t.CanStoreLargeNumberOfItemsInRandomlyOrder();
            }
            
           //  using var env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly());
           //  using var wtc = env.WriteTransaction();
           //  byte* buf = stackalloc byte[8192];
           //  var page = new Page(buf);
           //  var leaf = new SetLeafPage(page);
           //  leaf.Init(512);
           //
           // var list = new SortedList<int,int>();
           // var indexes = new int[] {23, 37, 12, 28};
           // var a = 812;
           //  for (int i = 0; i < 1024*16*100; i++)
           //  {
           //      //var a = 812 + (i%5 * 7) + i;
           //      a += indexes[i % indexes.Length];
           //
           //      if (leaf.Add(wtc.LowLevelTransaction, a) == false)
           //      {
           //          Validate(ref leaf, list.Keys);
           //          Console.WriteLine("Hey: " + i);
           //          return;
           //      }
           //
           //      list[a] = a;
           //      if(i % 1024 == 0)
           //          Validate(ref leaf, list.Keys);
           //  }
        }

        private static void Validate(ref SetLeafPage p, IList<int> expected)
        {
            Span<int> scratch = stackalloc int[128];
            var it = p.GetIterator(scratch);
            for (int i = 0; i < expected.Count; i++)
            {
           
                if (it.MoveNext(out long cur) == false ||
                    cur != expected[i])
                {
                    Console.WriteLine("Opps " + expected.Count);
                }
            }

            if (it.MoveNext(out long a))
            {
                Console.WriteLine("Um... " + expected.Count);
            }
        }
    }
}
