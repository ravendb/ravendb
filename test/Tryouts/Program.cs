using System;
using FastTests.Server.Documents.Expiration;

namespace Tryouts
{
    class Program
    {
        static void Main(string[] args)
        {
            for (int i = 0; i < 30; i++)
            {
                using (var t = new Expiration())
                {
                    Console.WriteLine(i);
                    t.CanAddALotOfEntitiesWithSameExpiry_ThenReadItBeforeItExpires_ButWillNotBeAbleToReadItAfterExpiry()
                        .Wait();
                }
            }
        } 
    }
}