using System;

namespace Tryouts
{
    public class Program
    {
        static void Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {
                
                Console.WriteLine(i);
                using (var a = new FastTests.Voron.LeafsCompression.RavenDB_5384())
                {
                    a.Leafs_compressed_CRUD(iterationCount: 26, size: 333, sequentialKeys: true, seed: 1);
                }
            }
        }
    }

}

