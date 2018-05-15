using System;
using System.Threading.Tasks;
using SlowTests.Client;

namespace Tryouts
{
    public static class Program
    {
        public static async Task Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine(i);
                using (var test = new UniqueValues())
                {
                    await test.CanPutUniqueString();
                }
            }
        }
    }
}
