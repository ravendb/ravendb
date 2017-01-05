using System;
using System.Diagnostics;
using System.Threading.Tasks;
using FastTests.Voron;
using FastTests.Voron.Streams;
using SlowTests.Voron;
using StressTests;
using Voron;
using Voron.Global;
using Voron.Impl.Scratch;
using Xunit;

namespace Tryouts
{
    public class Program
    {
        static void Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine(i);
                using (var a = new FastTests.Client.BulkInsert.BulkInserts())
                {
                    a.SimpleBulkInsertShouldWork().Wait();
                }

            }
            //using (var a = new CanUseStream())
            //{
            //    a.CanCopyTo(16897);
            //}
        }
    }


}

