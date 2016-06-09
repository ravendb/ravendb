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
                using (var x = new SlowTests.Voron.LargeFixedSizeTrees())
                {
                    x.CanDeleteRange_RandomRanges(count: 2000000, seed: 288291468);
                }

            }

        }
    }
}