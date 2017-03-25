using System;
using System.Diagnostics;
using FastTests.Client.Attachments;
using FastTests.Smuggler;
using System.Threading.Tasks;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine(Process.GetCurrentProcess().Id);
            Console.WriteLine();


            Parallel.For(0, 100, i =>
            {
                Console.WriteLine(i);
                using (var a = new FastTests.Server.Documents.Expiration.Expiration())
                {
                    a.CanAddALotOfEntitiesWithSameExpiry_ThenReadItBeforeItExpires_ButWillNotBeAbleToReadItAfterExpiry(count: 100).Wait();
                }
            });
        }
    }
}