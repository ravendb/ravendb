using System;
using RachisTests;

namespace Tryouts
{
    public static class Program
    {
        public static void Main(string[] args)
        {
            try
            {
                for (int i = 0; i < 500; i++)
                {
                    Console.WriteLine(i);

                    using (var test = new AddNodeToClusterTests())
                    {
                        test.PutDatabaseOnHealthyNodes().Wait();
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }
    }
}
