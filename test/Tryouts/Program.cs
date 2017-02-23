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
            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine(i);

                using (var a = new FastTests.Client.Indexing.StaticIndexesFromClient())
                {
                    a.Can_Put_And_Replace().Wait();
                }
            }
            
        }
    }
}