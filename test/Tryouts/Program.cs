using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using AsyncFriendlyStackTrace;
using Raven.Client.Document;
using Raven.Client.Smuggler;
using Raven.SlowTests.Issues;

namespace Tryouts
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            Console.WriteLine(IntPtr.Size);
            //for (int i = 0; i < 100; i++)
            {
                 
                {
                    Console.WriteLine(Thread.CurrentThread.ManagedThreadId);
                    var t = new FastTests.Sparrow.ByteString();
                    t.ConstructionInsideWholeSegment();
                }
            }
        }
    }
}