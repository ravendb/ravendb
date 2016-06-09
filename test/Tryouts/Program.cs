using System;
using FastTests.Server.Documents.Expiration;

namespace Tryouts
{
    class Program
    {
        static void Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine(i);
                var x = new SlowTests.Core.Bundles.ExpirationStressTest();
                {
                    x.CanAddALotOfEntitiesWithSameExpiry_ThenReadItBeforeItExpires_ButWillNotBeAbleToReadItAfterExpiry(
                        1000).Wait();
                }

            }

        }
    }
}