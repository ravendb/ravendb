using System;
using System.Threading.Tasks;
using SlowTests.Client;
using SlowTests.MailingList;

namespace Tryouts
{
    public static class Program
    {
        public static async Task Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine(i);
                using (var test = new Severin_null_data_time())
                {
                    test.QueryDateCompareTest();
                }
            }
        }
    }
}
